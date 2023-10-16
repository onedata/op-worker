%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The module handles streaming atm store dump.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_dump_download_utils).
-author("Bartosz Walkowicz").

-include("http/http_download.hrl").
-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-export([
    assert_operation_supported/1,
    handle_download/3
]).

-record(state, {
    auth :: atm_workflow_execution_auth:record(),
    iterator :: atm_store_container_iterator:record(),
    file_name :: binary(),
    any_item_streamed :: boolean()
}).
-type state() :: #state{}.

-define(LIST_BATCH_SIZE, 100).
-define(ID_PART_LEN, 12).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_operation_supported(atm_store:ctx()) -> ok | no_return().
assert_operation_supported(#atm_store_ctx{store = #atm_store{
    container = AtmStoreContainer
}}) ->
    %% TODO VFS-11424 add store dump iterators for other stores
    case atm_store_container:get_store_type(AtmStoreContainer) of
        audit_log -> ok;
        _ -> throw(?ERROR_NOT_SUPPORTED)
    end.


-spec handle_download(cowboy_req:req(), session:id(), atm_store:ctx()) ->
    cowboy_req:req() | no_return().
handle_download(Req, SessionId, #atm_store_ctx{
    id = AtmStoreId,
    store = AtmStore = #atm_store{workflow_execution_id = AtmWorkflowExecutionId},
    workflow_execution = AtmWorkflowExecution
}) ->
    SpaceId = AtmWorkflowExecution#atm_workflow_execution.space_id,

    State = #state{
        auth = atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, SessionId),
        iterator = atm_store_container:acquire_iterator(AtmStore#atm_store.container),
        file_name = gen_file_name(AtmStoreId, AtmStore, AtmWorkflowExecution),
        any_item_streamed = false
    },
    stream_store_content(Req, State).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gen_file_name(atm_store:id(), atm_store:record(), atm_workflow_execution:record()) ->
    binary().
gen_file_name(
    AtmStoreId,
    AtmStore = #atm_store{container = AtmStoreContainer, workflow_execution_id = AtmWorkflowExecutionId},
    AtmWorkflowExecution
) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    AtmStoreType = atm_store_container:get_store_type(AtmStoreContainer),
    AtmStoreName = sanitize_binary(get_store_name(AtmStoreId, AtmStore, AtmWorkflowExecution)),

    str_utils:format_bin("~s.~p.~s.~B-~2..0B-~2..0B_~2..0B-~2..0B-~2..0B.json", [
        binary:part(AtmWorkflowExecutionId, 0, 12), AtmStoreType, AtmStoreName,
        Year, Month, Day, Hour, Minute, Second
    ]).


%% @private
-spec get_store_name(atm_store:id(), atm_store:record(), atm_workflow_execution:record()) ->
    binary().
get_store_name(AtmStoreId, #atm_store{schema_id = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID}, _) ->
    % atm store has no reference to task containing it and as such getting the
    % task name would require to search through all task docs.
    % Returning part of store id is good enough.
    binary:part(AtmStoreId, 0, ?ID_PART_LEN);

get_store_name(_, #atm_store{schema_id = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID}, _) ->
    ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID;

get_store_name(
    _AtmStoreId,
    #atm_store{schema_id = AtmStoreSchemaId},
    #atm_workflow_execution{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}
) ->
    {ok, #document{value = #atm_workflow_schema_snapshot{
        revision = #atm_workflow_schema_revision{
            stores = AtmStoreSchemas
        }
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    {value, #atm_store_schema{name = AtmStoreName}} = lists:search(
        fun(#atm_store_schema{id = Id}) -> Id == AtmStoreSchemaId end,
        AtmStoreSchemas
    ),
    AtmStoreName.


%% @private
-spec sanitize_binary(binary()) -> binary().
sanitize_binary(Bin) ->
    << <<(sanitize_character(Char))/integer>> || <<Char>> <= Bin>>.


%% @private
-spec sanitize_character(integer()) -> integer().
sanitize_character(Char) when
    (Char >= $a andalso Char =< $z);
    (Char >= $A andalso Char =< $Z);
    (Char >= $0 andalso Char =< $9)
->
    Char;
sanitize_character(_) ->
    $-.


%% @private
-spec stream_store_content(cowboy_req:req(), state()) -> cowboy_req:req().
stream_store_content(Req1, State) ->
    Req2 = set_response_headers(Req1, State#state.file_name),

    Req3 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_TYPE => <<"application/json">>},
        Req2
    ),
    cowboy_req:stream_body(<<"[">>, nofin, Req3),
    stream_store_items(Req3, State),
    cowboy_req:stream_body(<<"]">>, fin, Req3),

    Req3.


%% @private
-spec set_response_headers(cowboy_req:req(), binary()) -> cowboy_req:req().
set_response_headers(Req1, FileName) ->
    Req2 = http_download_utils:set_cors_headers(Req1),
    http_download_utils:set_content_disposition_header(Req2, FileName).


%% @private
-spec stream_store_items(cowboy_req:req(), state()) -> ok.
stream_store_items(Req, State = #state{auth = Auth, iterator = Iterator}) ->
    case atm_store_container_iterator:get_next_batch(Auth, ?LIST_BATCH_SIZE, Iterator) of
        {ok, Items, NewIterator} ->
            NewState = lists:foldl(
                fun(Item, StateAcc) -> send_item(Req, Item, StateAcc) end,
                State#state{iterator = NewIterator},
                Items
            ),
            stream_store_items(Req, NewState);
        stop ->
            ok
    end.


%% @private
-spec send_item(cowboy_req:req(), automation:item(), state()) -> state().
send_item(Req, Item, State = #state{any_item_streamed = false}) ->
    send_data_chunk(Req, [encode_item(Item), <<"\r\n">>]),
    State#state{any_item_streamed = true};

send_item(Req, Item, State) ->
    send_data_chunk(Req, [<<",">>, encode_item(Item), <<"\r\n">>]),
    State.


%% @private
-spec encode_item(automation:item()) -> binary().
encode_item(Item) ->
    json_utils:encode(Item, [pretty]).


%% @private
-spec send_data_chunk(cowboy_req:req(), iodata()) -> ok.
send_data_chunk(Req, Data) ->
    http_download_utils:send_data_chunk(Data, Req, ?LIST_BATCH_SIZE, ?MIN_HTTP_SEND_RETRY_DELAY),
    ok.
