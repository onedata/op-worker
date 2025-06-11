%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client for space file events.
%%% @end
%%%-------------------------------------------------------------------
-module(space_file_events_test_sse_client).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([start/1, stop/1, get_events/1]).

-record(state, {
    test_process :: pid(),
    hackney_ref,
    events = [],
    sse_parser_state :: cow_sse:state()
}).


-define(START_TIMEOUT_MS, 2000).
-define(CALL_TIMEOUT_MS, 1000).
-define(AWAIT_TIMEOUT_MS, 10000).


%%%===================================================================
%%% API
%%%===================================================================


start(Args) ->
    TestProcess = self(),
    Pid = spawn(fun() -> client_process(TestProcess, Args) end),
    receive
        {Pid, ok} ->
            {ok, Pid};
        {Pid, {error, _} = Error} ->
            Error
    after ?START_TIMEOUT_MS ->
        exit(Pid, kill),
        ?ERROR_TIMEOUT
    end.


-spec stop(pid()) -> ok | ?ERROR_TIMEOUT.
stop(Pid) ->
    call(Pid, stop).


-spec get_events(pid()) -> {ok, [cow_sse:event()]} | ?ERROR_TIMEOUT.
get_events(Pid) ->
    call(Pid, get_events).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
call(Pid, Msg) ->
    Ref = make_ref(),
    Pid ! {{self(), Ref}, Msg},
    receive
        {Ref, ok} ->
            ok;
        {Ref, Response} ->
            {ok, Response}
    after ?CALL_TIMEOUT_MS ->
        exit(Pid, kill),
        ?ERROR_TIMEOUT
    end.


%% @private
client_process(TestProcess, Args) ->
    erlang:monitor(process, TestProcess),

    case send_request(Args) of
        {ok, Ref} ->
            State = #state{
                test_process = TestProcess,
                hackney_ref = Ref,
                sse_parser_state = cow_sse:init()
            },
            await_status(State);
        {error, _} = Error ->
            TestProcess ! {self(), Error}
    end.


%% @private
await_status(State = #state{test_process = TestProcess, hackney_ref = Ref}) ->
    receive
        {hackney_response, Ref, {status, 200, _Reason}} ->
            TestProcess ! {self(), ok},
            event_loop(State);
        {hackney_response, Ref, {status, Status, _Reason}} ->
            await_error_body(State, Status);
        {hackney_response, Ref, {error, _} = Error} ->
            TestProcess ! {self(), Error};
        {'DOWN', _, process, TestProcess, Reason} ->
            exit(Reason)
    after ?AWAIT_TIMEOUT_MS ->
        TestProcess ! {self(), {error, await_status_timeout}}
    end.


%% @private
await_error_body(State = #state{test_process = TestProcess, hackney_ref = Ref}, Status) ->
    receive
        {hackney_response, Ref, {headers, _Headers}} ->
            await_error_body(State, Status);
        {hackney_response, Ref, Bin} ->
            receive {hackney_response, Ref, done} -> ok end,
            Error = try
                #{<<"error">> := ErrorJson} = json_utils:decode(Bin),
                errors:from_json(ErrorJson)
            catch
                _:_ -> {undecodable_error_body, Bin}
            end,
            TestProcess ! {self(), {error, {Status, Error}}};
        {hackney_response, Ref, {error, _} = Error} ->
            TestProcess ! {self(), Error};
        {'DOWN', _, process, TestProcess, Reason} ->
            exit(Reason)
    after ?AWAIT_TIMEOUT_MS ->
        TestProcess ! {self(), {error, await_error_body_timeout}}
    end.


%% @private
event_loop(State = #state{
    test_process = TestProcess, hackney_ref = HackneyRef,
    events = Events, sse_parser_state = SSEState
}) ->
    receive
        {{From, TestProcessRef}, get_events} ->
            From ! {TestProcessRef, lists:reverse(Events)},
            event_loop(State#state{events = []});
        {{From, TestProcessRef}, stop} ->
            hackney:close(HackneyRef),
            From ! {TestProcessRef, ok};

        {hackney_response, HackneyRef, {headers, _}} ->
            event_loop(State);
        {hackney_response, HackneyRef, Bin} ->
            {NewSSEState, NewEvents} = decode_events(Bin, SSEState, Events),
            event_loop(State#state{
                events = NewEvents,
                sse_parser_state = NewSSEState
            });
        {hackney_response, HackneyRef, done} ->
            exit(normal);
        {hackney_response, HackneyRef, {error, _Reason}} ->
            exit({shutdown, {hackney_error, _Reason}});
        {'DOWN', _, process, TestProcess, Reason} ->
            hackney:close(HackneyRef),
            exit(Reason)
    end.


%% @private
send_request(Args = #{
    node := Node,
    space_id := SpaceId,
    token := Token
}) ->
    Path = <<"spaces/", SpaceId/binary, "/events/files">>,

    HeadersWithAuth = [rest_test_utils:user_token_header(Token)],

    Payload = case maps:get(body_bin, Args, undefined) of
        undefined ->
            BodyJson = case maps:get(body_json, Args, undefined) of
                undefined ->
                    RequiredBody = #{
                        <<"observedDirectories">> => lists:map(fun(DirGuid) ->
                            {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),
                            DirObjectId
                        end, maps:get(observed_dirs, Args))
                    },
                    case maps:get(observed_attrs, Args, undefined) of
                        undefined ->
                            RequiredBody;
                        AttrsToObserve ->
                            RequiredBody#{
                                <<"observedAttributes">> => lists:map(
                                    fun onedata_file:attr_name_to_json/1,
                                    AttrsToObserve
                                )
                            }
                    end;
                Json ->
                    Json
            end,
            json_utils:encode(BodyJson);
        EncodedBody ->
            EncodedBody
    end,

    Opts = [
%%        {recv_timeout, 50000},
        async
    ],
    rest_test_utils:request(Node, Path, post, HeadersWithAuth, Payload, Opts).


%% @private
decode_events(Data, State, EventsAcc) ->
    case cow_sse:parse(Data, State) of
        {event, Event, NewState} ->
            decode_events(<<>>, NewState, [Event | EventsAcc]);
        {more, NewState} ->
            {NewState, EventsAcc}
    end.
