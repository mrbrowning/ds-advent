use proc_macro::TokenStream;
use quote::quote;
use syn::{DataEnum, DeriveInput};

#[proc_macro_attribute]
pub fn maelstrom_message(_: TokenStream, annotated_item: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(annotated_item).unwrap();
    let enum_data: Option<&DataEnum> = match &ast.data {
        syn::Data::Struct(_) => {
            panic!("maelstrom_message must be applied to an enum");
        }
        syn::Data::Enum(e) => Some(e),
        syn::Data::Union(_) => {
            panic!("maelstrom_message must be applied to an enum");
        }
    };

    let attrs = ast.attrs;
    let msg_enum = enum_data.unwrap();
    let enum_ident = ast.ident;
    let variants = &msg_enum.variants;
    let gen = quote! {
        #( #attrs )*
        #[serde(tag = "type")]
        enum #enum_ident {
            #variants

            #[serde(rename = "init")]
            Init(InitMessagePayload),

            #[serde(rename = "init_ok")]
            InitOk,

            #[serde(rename = "error")]
            Error(ErrorMessagePayload),

            #[serde(rename = "empty")]
            Empty,
        }

        impl Default for #enum_ident {
            fn default() -> Self {
                Self::Empty
            }
        }

        impl MessagePayload for #enum_ident {
            fn as_init_msg(&self) -> Option<InitMessagePayload> {
                match self {
                    Self::Init(m) => Some(m.clone()),
                    _ => None,
                }
            }

            fn to_init_ok_msg() -> Self {
                Self::InitOk
            }

            fn to_err_msg(err: ErrorMessagePayload) -> Self {
                Self::Error(err)
            }
        }
    };

    TokenStream::from(gen)
}
