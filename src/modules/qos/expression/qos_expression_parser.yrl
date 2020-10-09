%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is an input file for `yecc` - An LALR-1 parser generator 
%%% for Erlang, similar to yacc. 
%%% From this file `qos_expression_parser.erl` file is generated. 
%%% More info about this file format, generating and usage of generated 
%%% module can be found in README.md
%%% @end
%%%--------------------------------------------------------------------

Nonterminals
expression value.

Terminals '(' ')' 
number string operator comparator eq any_storage.

Rootsymbol expression.

Left 100 operator.
Nonassoc 200 '('.

value -> string : '$1'.
value -> number : '$1'.

expression -> string comparator number : {unwrap('$2'), unwrap('$1'), unwrap('$3')}.
expression -> string eq value : {unwrap('$2'), unwrap('$1'), unwrap('$3')}.
expression -> any_storage : unwrap('$1').
expression -> expression operator expression : {unwrap('$2'), '$1', '$3'}.
expression -> '(' expression ')' : '$2'.

Erlang code.

unwrap({_,_,V}) -> V.
