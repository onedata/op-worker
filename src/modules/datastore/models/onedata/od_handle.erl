%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @doc Cache for handle details fetched from OZ.
%%% @end
%%%-------------------------------------------------------------------
-module(od_handle).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/oz/oz_handles.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([actual_timestamp/0]).
-export([save/1, get/1, get_or_fetch/2, list/0, exists/1, delete/1, update/2,
    create/1, create_or_update/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type id() :: binary().
-type info() :: #od_handle{}.
-type doc() :: datastore_doc:doc(info()).
-type diff() :: datastore_doc:diff(info()).
-type resource_type() :: binary().
-type resource_id() :: binary().
-type public_handle() :: binary().
-type metadata() :: binary().
-type timestamp() :: calendar:datetime().

-export_type([doc/0, info/0, id/0]).
-export_type([resource_type/0, resource_id/0, public_handle/0, metadata/0,
    timestamp/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv universaltime().
%%--------------------------------------------------------------------
-spec actual_timestamp() -> timestamp().
actual_timestamp() ->
    erlang:universaltime().

%%--------------------------------------------------------------------
%% @doc
%% Saves handle.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates handle.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates handle.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, id()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Returns handle.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes handle.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether group exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context. If space info is not found
%% fetches it from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), HandleId :: id()) ->
    {ok, datastore:doc()} | {error, term()}.
get_or_fetch(Auth, HandleId) ->
    case od_handle:get(HandleId) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(Auth, HandleId);
        {error, Reason} -> {error, Reason}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches space info from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Auth :: oz_endpoint:auth(), HandleId :: id()) ->
    {ok, datastore:doc()} | {error, term()}.
fetch(?GUEST_SESS_ID = Auth, HandleId) ->
    {ok, #handle_details{
        public_handle = PublicHandle,
        metadata = Metadata
    }} = oz_handles:get_public_details(Auth, HandleId),

    Doc = #document{key = HandleId, value = #od_handle{
        public_handle = PublicHandle,
        metadata = Metadata
    }},

    case create(Doc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, Doc};

fetch(Auth, HandleId) ->
    {ok, #handle_details{
        handle_service = HandleService,
        public_handle = PublicHandle,
        resource_type = ResourceType,
        resource_id = ResourceId,
        metadata = Metadata,
        timestamp = Timestamp
    }} = oz_handles:get_details(Auth, HandleId),

    Doc = #document{key = HandleId, value = #od_handle{
        handle_service = HandleService,
        public_handle = PublicHandle,
        resource_type = ResourceType,
        resource_id = ResourceId,
        metadata = Metadata,
        timestamp = Timestamp
    }},

    case create(Doc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, Doc}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {public_handle, string},
        {resource_type, string},
        {resource_id, string},
        {metadata, string},
        {timestamp, {{integer, integer, integer}, {integer, integer, integer}}},
        {handle_service, string},
        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},
        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},
        {revision_history, [term]}
    ]}.
