## QoS expression 

QoS expression scanner and parser are generated using `leex` and `yecc`.

`qos_expression_scanner.xrl` is an input file for `leex` - Lexical analyzer generator for Erlang. From this file `qos_expression_scanner.erl` file is generated.

`qos_expression_parser.yrl` is and input file for `yecc` - An LALR-1 parser generator for Erlang, similar to yacc. From this file `qos_expression_parser.erl` file is generated. 

Input files format and more info can be found here - [leex](https://erlang.org/doc/man/leex.html), [yeec](https://erlang.org/doc/man/yecc.html).

#### Generating scanner and parser
`qos_expression_scanner` and `qos_expression_parser` modules are automatically generated during compilation of `op_worker` with `rebar`.

Can be also generated manually:
* qos_expression_scanner: \
`erl -I -noshell -eval 'leex:file("qos_expression_scanner"), halt().'`
* qos_expression_parser: \
`erl -I -noshell -eval 'yecc:file("qos_expression_parser"), halt().'` 


#### Usage

Module `qos_expression_scanner` exports function `string/1` which takes input as a string(Erlang list) and returns list of tokens.

Those tokens are an input to `qos_expression_parser` function `parse/1`.

These modules are used in `qos_expression` module.