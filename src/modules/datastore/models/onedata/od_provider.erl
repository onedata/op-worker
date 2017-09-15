%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Cache for space details fetched from onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(od_provider).
-author("Michal Zmuda").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_or_update/2, get_or_fetch/1, fetch/1]).
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type id() :: binary().
-type info() :: #od_provider{}.
-type doc() :: datastore_doc:doc(info()).
-type diff() :: datastore_doc:diff(info()).

-export_type([id/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves provider.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates provider.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates provider.
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
%% Returns provider.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes provider.
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
%% Get provider from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(ProviderId :: id()) ->
    {ok, datastore:doc()} | {error, term()}.
get_or_fetch(ProviderId) ->
    case od_provider:get(ProviderId) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(ProviderId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetch provider from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ProviderId :: id()) ->
    {ok, datastore:doc()} | {error, Reason :: term()}.
fetch(ProviderId) ->
    try
        {ok, #provider_details{name = Name, urls = URLs}} =
            oz_providers:get_details(provider, ProviderId),
        {PublicOnly, SpaceIDs} = case oz_providers:get_spaces(provider) of
            {ok, SIDs} -> {false, SIDs};
            {error, Res} ->
                ?warning("Unable to fetch public info for provider ~p due to ~p", [
                    ProviderId, Res]),
                {true, []}
        end,

        Doc = #document{key = ProviderId, value = #od_provider{
            client_name = Name,
            urls = URLs,
            spaces = SpaceIDs,
            public_only = PublicOnly
        }},

        case od_provider:create(Doc) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end,
        {ok, Doc}
    catch
        _:Reason ->
            {error, Reason}
    end.

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
        {client_name, string},
        {urls, [string]},
        {spaces, [string]},
        {public_only, boolean},
        {revision_history, [term]}
    ]}.