-module(strona).
-compile(export_all).
-include_lib("n2o/include/wf.hrl").

main() ->
    #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}.

title() -> [<<"BLABLABLA">>].

body() ->
    wf:comet(fun() -> chat_loop() end),
    [
        #button{id = send, body = <<"Goozik">>, postback = goozik},
        #panel{id = history, body = gui_utils:get_requested_hostname()}
    ].

event(init) ->
    wf:reg(room);

event(goozik) ->
    io:format("goozik pid: ~p~n", [self()]).

chat_loop() ->
    Terms = [#span{body = lists:flatten(io_lib:format("~p", [self()]))}, #br{}],
    wf:insert_bottom(history, Terms),
    wf:flush(room),
    timer:sleep(2000),
    chat_loop().
