%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc datastore worker's implementation
%%% @end
%%%--------------------------------------------------------------------
-module(dbsync_worker).
-author("Rafal Slota").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).
-export([state_get/1, state_put/2]).

-define(MODELS_TO_SYNC, [file_meta, file_location]).

-record(change, {
    seq,
    doc
}).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    timer:sleep(5000),
    ?info("[ DBSync ]: Starting dbsync..."),
    {ok, ChangesStream} = init_stream(0, infinity, global),
    {ok, #{changes_stream => ChangesStream}}.

init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    couchdb_datastore_driver:changes_start_link(
        fun(Seq, Doc) ->
            worker_proxy:call(dbsync_worker, {Queue, #change{seq = Seq, doc = Doc}})
        end, Since, Until).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck |
    {driver_call, Module :: atom(), Method :: atom(), Args :: [term()]},
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;


handle({Queue, #change{seq = Seq, doc = #document{key = Key} = Doc}}) ->
    ?info("[ DBSync ]: Received change on queue ~p with seq ~p: ~p", [Queue, Seq, Doc]),
    case is_file_scope(Doc) of
        true ->
            case get_space_id(Doc) of
                {ok, SpaceId} ->
                    ?info("Document ~p assigned to space ~p", [Key, SpaceId]);
                {error, Reason} ->
                    ?error("Unable to find space id for document ~p", [Doc]),
                    {error, Reason}
            end ;
        false ->
            ok
    end;

%% Unknown request
handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_put(Key :: term(), Value :: term()) -> ok.
state_put(Key, Value) ->
    worker_host:state_put(?MODULE, Key, Value).

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    worker_host:state_get(?MODULE, Key).


is_file_scope(#document{value = #links{model = ModelName}}) ->
    lists:member(ModelName, ?MODELS_TO_SYNC);
is_file_scope(#document{value = Value}) when is_tuple(Value) ->
    ModelName = element(1, Value),
    lists:member(ModelName, ?MODELS_TO_SYNC).


get_file_scope(#document{key = Key, value = #file_meta{}}) ->
    Key;
get_file_scope(#document{value = #links{key = DocKey, model = file_meta}}) ->
    DocKey;
get_file_scope(#document{value = #links{key = DocKey, model = file_location}}) ->
    #model_config{store_level = StoreLevel} = file_location:model_init(),
    {ok, #document{value = #file_location{}} = Doc} = datastore:get(StoreLevel, file_location, DocKey),
    get_file_scope(Doc);
get_file_scope(#document{value = #file_location{uuid = FileUUID}}) ->
    FileUUID.


get_space_id(#document{key = Key} = Doc) ->
    case state_get({space_id, Key}) of
        undefined ->
            get_space_id_not_cached(Key, Doc);
        SpaceId ->
            {ok, SpaceId}
    end.

get_space_id_not_cached(KeyToCache, #document{} = Doc) ->
    FileUUID = get_file_scope(Doc),
    case file_meta:get_scope({uuid, FileUUID}) of
        {ok, #document{key = SpaceId}} ->
            state_put({space_id, KeyToCache}, SpaceId),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.