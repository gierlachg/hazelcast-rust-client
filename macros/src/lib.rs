extern crate proc_macro;

use proc_macro2::TokenStream;
use syn::{parse_macro_input, spanned::Spanned, Attribute, Data, DeriveInput, Fields, Lit, Meta, Type};

use quote::{quote, quote_spanned};

#[proc_macro_derive(Request, attributes(r#type))]
pub fn derive_request(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let request_body = request_body(&input);
    let writer_body = writer_body(&input);
    let expanded = quote! {
        #request_body

        #writer_body
    };
    proc_macro::TokenStream::from(expanded)
}

fn request_body(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let type_value = find_attribute_value("r#type", &input.attrs).expect("missing 'type' attribute!");

    quote! {
        impl #impl_generics crate::messaging::Request for #name #ty_generics #where_clause {
            fn r#type() -> u16 {
                #type_value
            }
        }
    }
}

#[proc_macro_derive(Writer)]
pub fn derive_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let writer_body = writer_body(&input);
    let expanded = quote! {
        #writer_body
    };
    proc_macro::TokenStream::from(expanded)
}

fn writer_body(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let length_body = length_body(&input.data);
    let write_to_body = write_to_body(&input.data);

    quote! {
        impl #impl_generics crate::codec::Writer for #name #ty_generics #where_clause {
            fn length(&self) -> usize {
                #length_body
            }

            fn write_to(&self, writeable: &mut dyn crate::codec::Writeable) {
                #write_to_body
            }
        }
    }
}

fn length_body(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|field| {
                    let name = &field.ident;
                    quote_spanned! {field.span() =>
                        self.#name.length()
                    }
                });
                quote! {
                    0 #(+ #recurse)*
                }
            }
            Fields::Unnamed(_) | Fields::Unit => unimplemented!(),
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

fn write_to_body(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|field| {
                    let name = &field.ident;
                    quote_spanned! {field.span() =>
                        self.#name.write_to(writeable);
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(_) | Fields::Unit => unimplemented!(),
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

#[proc_macro_derive(Response, attributes(r#type))]
pub fn derive_response(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let response_body = response_body(&input);
    let reader_body = reader_body(&input);
    let expanded = quote! {
        #response_body

        #reader_body
    };
    proc_macro::TokenStream::from(expanded)
}

fn response_body(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let type_value = find_attribute_value("r#type", &input.attrs).expect("missing 'type' attribute!");

    quote! {
        impl #impl_generics crate::messaging::Response for #name #ty_generics #where_clause {
            fn r#type() -> u16 {
                 #type_value
            }
        }
    }
}

#[proc_macro_derive(Reader)]
pub fn derive_reader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let reader_body = reader_body(&input);
    let expanded = quote! {
        #reader_body
    };
    proc_macro::TokenStream::from(expanded)
}

fn reader_body(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let read_from_body = read_from_body(&input.data);

    quote! {
        impl #impl_generics crate::codec::Reader for #name #ty_generics #where_clause {
            fn read_from(readable: &mut dyn crate::codec::Readable) -> Self {
                #name {
                    #read_from_body
                }
            }
        }
    }
}

fn read_from_body(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|field| match &field.ty {
                    Type::Path(type_path) => {
                        let name = &field.ident;
                        let type_name = &type_path.path.segments.first().expect("missing first segment!").ident;
                        quote_spanned! {field.span() =>
                            #name: #type_name::read_from(readable),
                        }
                    }
                    Type::Array(_)
                    | Type::BareFn(_)
                    | Type::Group(_)
                    | Type::ImplTrait(_)
                    | Type::Infer(_)
                    | Type::Macro(_)
                    | Type::Never(_)
                    | Type::Paren(_)
                    | Type::Ptr(_)
                    | Type::Reference(_)
                    | Type::Slice(_)
                    | Type::TraitObject(_)
                    | Type::Tuple(_)
                    | Type::Verbatim(_) => unimplemented!(),
                    _ => unimplemented!(),
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(_) | Fields::Unit => unimplemented!(),
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

fn find_attribute_value(name: &str, attributes: &Vec<Attribute>) -> Option<Lit> {
    attributes
        .iter()
        .map(|attribute| attribute.parse_meta().expect("unable to parse attribute!"))
        .find_map(|meta| match meta {
            Meta::NameValue(value) => {
                if value.path.segments.first().expect("missing attribute name!").ident == name {
                    Some(value.lit)
                } else {
                    None
                }
            }
            Meta::Path(_) | Meta::List(_) => unimplemented!(),
        })
}
