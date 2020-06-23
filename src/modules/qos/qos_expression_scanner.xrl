%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------

Definitions.

Digit       = [0-9]
Char        = [A-Za-z0-9_]
Middle      = [\s-]
Whitespace  = ([\000-\s]|%.*)
Comparator  = (<|<=|>=|>)
Operator    = [|&\\]


Rules.

{Operator}                              : {token, {operator, TokenLine, list_to_binary(TokenChars)}}.
[=]                                     : {token, {eq, TokenLine, list_to_binary(TokenChars)}}.
{Comparator}                            : {token, {comparator, TokenLine, list_to_binary(TokenChars)}}.
anyStorage                              : {token, {any_storage, TokenLine, list_to_binary(TokenChars)}}.
{Digit}+                                : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{Char}({Char}|{Middle})*{Char}|{Char}   : {token, {string, TokenLine, list_to_binary(TokenChars)}}.
[()]                                    : {token, {list_to_atom(TokenChars), TokenLine}}.
{Whitespace}+                           : skip_token.
\"|\"                                   : skip_token.


Erlang code.

