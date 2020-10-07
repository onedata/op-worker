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
Char        = [\x{00A0}-\x{1FFF}a-zA-Z0-9_]
Middle      = [\s-]
Whitespace  = ([\000-\s]|%.*)
Comparator  = (<|<=|>=|>)
Operator    = [|&\\]


Rules.

{Operator}                                  : {token, {operator, TokenLine, TokenChars}}.
[=]                                         : {token, {eq, TokenLine, TokenChars}}.
{Comparator}                                : {token, {comparator, TokenLine, TokenChars}}.
anyStorage                                  : {token, {any_storage, TokenLine, TokenChars}}.
{Digit}+                                    : {token, {number, TokenLine, list_to_integer(TokenChars)}}.
{Digit}+\.{Digit}+((E|e)(\+|\-)?{Digit}+)?  : {token, {number, TokenLine, list_to_float(TokenChars)}}.
{Char}({Char}|{Middle})*{Char}|{Char}       : {token, {string, TokenLine, TokenChars}}.
[()]                                        : {token, {list_to_atom(TokenChars), TokenLine}}.
{Whitespace}+                               : skip_token.
\"|\"                                       : skip_token.


Erlang code.

