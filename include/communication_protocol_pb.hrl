-ifndef(CLUSTERMSG_PB_H).
-define(CLUSTERMSG_PB_H, true).
-record(clustermsg, {
    module_name = erlang:error({required, module_name}),
    message_type = erlang:error({required, message_type}),
    answer_type = erlang:error({required, answer_type}),
    synch = erlang:error({required, synch}),
    protocol_version = erlang:error({required, protocol_version}),
    input
}).
-endif.

-ifndef(ANSWER_PB_H).
-define(ANSWER_PB_H, true).
-record(answer, {
    answer_status = erlang:error({required, answer_status}),
    worker_answer
}).
-endif.

-ifndef(ATOM_PB_H).
-define(ATOM_PB_H, true).
-record(atom, {
    value = erlang:error({required, value})
}).
-endif.

