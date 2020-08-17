%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is an input file for `leex` - lexical analyzer generator 
%%% for Erlang - concerning QoS expression.
%%% From this file `qos_expression_scanner.erl` module will be generated.
%%% More info about this file format, generating and usage of generated 
%%% module can be found in README.md
%%% @end
%%%--------------------------------------------------------------------

Definitions.

Digit       = [0-9]
Char        = [A-Za-z0-9_]
Middle      = [\s-]
Whitespace  = ([\000-\s]|%.*)
Comparator  = (<|<=|>=|>)
Operator    = [|&\\]


Rules.

{Operator}                              : {token, {operator, TokenLine, convert(TokenChars)}}.
[=]                                     : {token, {eq, TokenLine, convert(TokenChars)}}.
{Comparator}                            : {token, {comparator, TokenLine, convert(TokenChars)}}.
anyStorage                              : {token, {any_storage, TokenLine, convert(TokenChars)}}.
{Digit}+                                : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{Char}({Char}|{Middle})*{Char}|{Char}   : {token, {string, TokenLine, convert(TokenChars)}}.
[()]                                    : {token, {list_to_atom(TokenChars), TokenLine}}.
{Whitespace}+                           : skip_token.
\"|\"                                   : skip_token.


Erlang code.

convert(Token) ->
    str_utils:unicode_list_to_binary(Token).