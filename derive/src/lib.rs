use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Expr, Variant};

#[proc_macro_derive(ErrCode, attributes(status, value, error))]
pub fn derive_err_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let enum_name = &input.ident;
    let generics = &input.generics;

    let variants = match &input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        _ => {
            return syn::Error::new_spanned(
                &input,
                "ErrCode derive macro can only be used on enums",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut status_arms = Vec::new();
    let mut value_arms = Vec::new();
    let mut response_arms = Vec::new();

    for variant in variants {
        let variant_name = &variant.ident;
        let variant_str = variant_name.to_string();

        let status = parse_status_attr(variant).unwrap_or(500u16);
        let value = parse_value_attr(variant).unwrap_or(status as u64);
        let error_msg = parse_error_attr(variant)
            .unwrap_or_else(|| variant_str.clone());

        let (field_pattern, field_count) = match &variant.fields {
            syn::Fields::Unit => {
                (
                    quote! { #enum_name::#variant_name },
                    0usize,
                )
            }
            syn::Fields::Unnamed(fields) => {
                let field_count = fields.unnamed.len();
                if field_count == 1 {
                    (
                        quote! { #enum_name::#variant_name(ref field0) },
                        1usize,
                    )
                } else {
                    let field_names: Vec<_> = (0..field_count)
                        .map(|i| syn::Ident::new(&format!("field{}", i), proc_macro2::Span::call_site()))
                        .collect();
                    (
                        quote! { #enum_name::#variant_name(#(ref #field_names),*) },
                        field_count,
                    )
                }
            }
            syn::Fields::Named(_) => {
                return syn::Error::new_spanned(
                    variant,
                    "Named fields are not supported",
                )
                .to_compile_error()
                .into();
            }
        };

        let is_unit = matches!(variant.fields, syn::Fields::Unit);

        status_arms.push(quote! {
            #field_pattern => #status,
        });

        value_arms.push(quote! {
            #field_pattern => #value,
        });

        // to_response() 구현
        if is_unit {
            response_arms.push(quote! {
                #field_pattern => {
                    let status = #status;
                    let value = #value;
                    let msg = #error_msg;
                    let reason = msg.replace("{0}", "").replace("{1}", "").replace("{2}", "")
                        .replace("{3}", "").replace("{4}", "").replace("{5}", "")
                        .trim().to_string();
                    let json = serde_json::json!({
                        "status": value,
                        "reason": reason
                    });
                    (status, json)
                },
            });
        } else if field_count == 1 {
            response_arms.push(quote! {
                #field_pattern => {
                    let status = #status;
                    let value = #value;
                    let msg = #error_msg;
                    let reason = if msg.contains("{0}") {
                        msg.replace("{0}", &format!("{}", field0))
                    } else {
                        format!("{}: {}", msg, field0)
                    };
                    let json = serde_json::json!({
                        "status": value,
                        "reason": reason
                    });
                    (status, json)
                },
            });
        } else {
            // 여러 필드 처리
            let field_names: Vec<_> = (0..field_count)
                .map(|i| syn::Ident::new(&format!("field{}", i), proc_macro2::Span::call_site()))
                .collect();
            
            let placeholder_replacements: Vec<_> = (0..field_count)
                .map(|i| {
                    let placeholder = format!("{{{}}}", i);
                    let field_name = &field_names[i];
                    quote! {
                        reason = reason.replace(#placeholder, &format!("{}", #field_name));
                    }
                })
                .collect();

            response_arms.push(quote! {
                #field_pattern => {
                    let status = #status;
                    let value = #value;
                    let mut reason = #error_msg.to_string();
                    #(#placeholder_replacements)*
                    let json = serde_json::json!({
                        "status": value,
                        "reason": reason
                    });
                    (status, json)
                },
            });
        }
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics crate::cassry::ErrCode for #enum_name #ty_generics #where_clause {
            fn status(&self) -> u16 {
                match self {
                    #(#status_arms)*
                }
            }

            fn value(&self) -> u64 {
                match self {
                    #(#value_arms)*
                }
            }

            fn to_response(&self) -> (u16, serde_json::Value) {
                match self {
                    #(#response_arms)*
                }
            }
        }
    };

    TokenStream::from(expanded)
}

fn parse_status_attr(variant: &Variant) -> Option<u16> {
    for attr in &variant.attrs {
        if attr.path().is_ident("status") {
            if let Ok(expr) = attr.parse_args::<Expr>() {
                if let Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(lit_int),
                    ..
                }) = expr
                {
                    return lit_int.base10_parse().ok();
                }
            }
        }
    }
    None
}

fn parse_value_attr(variant: &Variant) -> Option<u64> {
    for attr in &variant.attrs {
        if attr.path().is_ident("value") {
            if let Ok(expr) = attr.parse_args::<Expr>() {
                if let Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(lit_int),
                    ..
                }) = expr
                {
                    return lit_int.base10_parse().ok();
                }
            }
        }
    }
    None
}

fn parse_error_attr(variant: &Variant) -> Option<String> {
    // #[error(...)] attribute를 찾습니다
    // 이 attribute는 thiserror와 ErrCode derive가 모두 사용할 수 있습니다
    // 먼저 찾은 것을 사용합니다 (일반적으로 thiserror가 먼저 적용되므로 thiserror의 것이 먼저 찾아질 수 있습니다)
    for attr in &variant.attrs {
        if attr.path().is_ident("error") {
            if let Ok(expr) = attr.parse_args::<Expr>() {
                if let Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit_str),
                    ..
                }) = expr
                {
                    // thiserror의 포맷 문자열을 반환 (예: "JWT verification failed: {0}")
                    return Some(lit_str.value());
                }
            }
        }
    }
    None
}

