-module(opn_page_handler).

-include_lib("ctool/include/logging.hrl").

-export([is_html_req/1, handle_html_req/1, handle_ws_req/1]).


-define(WEBSOCKET_PREFIX_PATH, "/ws/").

-define(MSG_TYPE_KEY, <<"msgType">>).
-define(PULL_REQ_VAL, <<"pullReq">>).
-define(PULL_RESP_VAL, <<"pullResp">>).

-define(UUID_KEY, <<"uuid">>).

-define(ENTITY_TYPE_KEY, <<"entityType">>).

-define(ENTITY_IDS_KEY, <<"entityIds">>).

-define(OPERATION_KEY, <<"operation">>).
-define(FIND_VAL, <<"find">>).
-define(FIND_MANY_VAL, <<"findMany">>).
-define(FIND_ALL_VAL, <<"findAll">>).
-define(FIND_QUERY_VAL, <<"findQuery">>).
-define(CREATE_RECORD_VAL, <<"createRecord">>).
-define(UPDATE_RECORD_VAL, <<"updateRecord">>).
-define(DELETE_RECORD_VAL, <<"deleteRecord">>).

-define(RESULT_KEY, <<"result">>).
-define(OK_VAL, <<"ok">>).
-define(ERROR_VAL, <<"error">>).

-define(DATA_KEY, <<"data">>).
-define(INTERNAL_SERVER_ERROR_VAL, <<"Internal Server Error">>).





is_html_req(Req) ->
    {FullPath, _} = cowboy_req:path(Req),
    Path = case FullPath of
               <<?WEBSOCKET_PREFIX_PATH, P/binary>> ->
                   P;
               <<"/", P/binary>> ->
                   P
           end,
    case byte_size(Path) >= 5 andalso binary:split(Path, <<"/">>) =:= [Path] of
        false ->
            false;
        true ->
            case binary_part(Path, {byte_size(Path), -5}) of
                <<".html">> ->
                    true;
                _ ->
                    false
            end
    end.


handle_html_req(Req) ->
    g_ctx:init_context(Req),
    ok.


handle_ws_req(Props) ->
    MsgUUID = proplists:get_value(?UUID_KEY, Props, null),
    try
        ?PULL_REQ_VAL = proplists:get_value(?MSG_TYPE_KEY, Props),
        Data = proplists:get_value(?DATA_KEY, Props),
        EntityType = proplists:get_value(?ENTITY_TYPE_KEY, Props),
        EntityIdOrIds = proplists:get_value(?ENTITY_IDS_KEY, Props),
        {Result, RespData} =
            case proplists:get_value(?OPERATION_KEY, Props) of
                ?FIND_VAL ->
                    handle_find_req(EntityType, [EntityIdOrIds]);
                ?FIND_MANY_VAL ->
                    handle_find_req(EntityType, EntityIdOrIds);
                ?FIND_ALL_VAL ->
                    handle_find_all_req(EntityType);
                ?FIND_QUERY_VAL ->
                    handle_find_query_req(EntityType, Data);
                ?CREATE_RECORD_VAL ->
                    handle_create_record_req(EntityType, Data);
                ?UPDATE_RECORD_VAL ->
                    handle_update_record_req(EntityType, EntityIdOrIds, Data);
                ?DELETE_RECORD_VAL ->
                    handle_delete_record_req(EntityType, EntityIdOrIds)
            end,
        ResultVal = case Result of
                        ok -> ?OK_VAL;
                        error -> ?ERROR_VAL
                    end,
        [
            {?MSG_TYPE_KEY, ?PULL_RESP_VAL},
            {?UUID_KEY, MsgUUID},
            {?RESULT_KEY, ResultVal},
            {?DATA_KEY, RespData}
        ]
    catch T:M ->
        ?error_stacktrace("Error while handling websocket message - ~p:~p",
            [T, M]),
        [
            {?MSG_TYPE_KEY, ?PULL_RESP_VAL},
            {?UUID_KEY, MsgUUID},
            {?RESULT_KEY, ?ERROR_VAL},
            {?DATA_KEY, ?INTERNAL_SERVER_ERROR_VAL}
        ]
    end.


handle_find_req(EntityType, Ids) ->
    call_page_handler(find, [EntityType, Ids]).

handle_find_all_req(EntityType) ->
    call_page_handler(find_all, [EntityType]).

handle_find_query_req(EntityType, Data) ->
    call_page_handler(find_query, [EntityType, Data]).

handle_create_record_req(EntityType, Data) ->
    call_page_handler(create_record, [EntityType, Data]).

handle_update_record_req(EntityType, Id, Data) ->
    case call_page_handler(update_record, [EntityType, Id, Data]) of
        ok -> {ok, null};
        {error, Msg} -> {error, Msg}
    end.

handle_delete_record_req(EntityType, Id) ->
    case call_page_handler(delete_record, [EntityType, Id]) of
        ok -> {ok, null};
        {error, Data} -> {error, Data}
    end.

call_page_handler(Fun, Args) ->
    Module = g_ctx:page_module(),
    erlang:apply(Module, Fun, Args).