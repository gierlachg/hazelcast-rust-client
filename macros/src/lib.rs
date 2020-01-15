extern crate proc_macro;

use proc_macro2::TokenStream;
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, Type};

use quote::{quote, quote_spanned};

#[proc_macro_derive(Writer)]
pub fn derive_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let body = writer_body(&input.data);

    let expanded = quote! {
        impl #impl_generics Writer for #name #ty_generics #where_clause {
            fn write_to(&self, writeable: &mut dyn Writeable) {
                #body
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

fn writer_body(data: &Data) -> TokenStream {
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

#[proc_macro_derive(Reader)]
pub fn derive_reader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let body = reader_body(&input.data);

    let expanded = quote! {
        impl #impl_generics Reader for #name #ty_generics #where_clause {
            fn read_from(readable: &mut dyn Readable) -> Self {
                #name {
                    #body
                }
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

fn reader_body(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let recurse = fields.named.iter().map(|field| match &field.ty {
                    Type::Path(type_path) => {
                        let name = &field.ident;
                        let type_name = &type_path
                            .path
                            .segments
                            .first()
                            .expect("missing first segment!")
                            .ident;
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
