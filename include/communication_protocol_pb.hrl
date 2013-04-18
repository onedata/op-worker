-ifndef(CLUSTERMSG_PB_H).
-define(CLUSTERMSG_PB_H, true).
-record(clustermsg, {
    module_name = erlang:error({required, module_name}),
    message_type = erlang:error({required, message_type}),
    input = erlang:error({required, input})
}).
-endif.

-ifndef(ATOM_PB_H).
-define(ATOM_PB_H, true).
-record(atom, {
    atom = erlang:error({required, atom})
}).
-endif.

