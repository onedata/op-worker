%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for storing dir's location data
%%% @end
%%%-------------------------------------------------------------------
-module(dir_location).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([delete/1, mark_dir_created_on_storage/2, is_storage_file_created/1, get/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type key() :: file_meta:uuid().
-type dir_location() :: #dir_location{}.
-type doc() :: datastore:doc(dir_location()).

-export_type([key/0, dir_location/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    routing => global
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Marks given dir as created on storage
%% @end
%%--------------------------------------------------------------------
-spec mark_dir_created_on_storage(key(), od_space:id()) -> {ok, key()} |{error, term()}.
mark_dir_created_on_storage(FileUuid, SpaceId) ->
    Doc = #document{
        key = FileUuid,
        value = #dir_location{storage_file_created = true},
        scope = SpaceId
    },
    datastore_model:create(?CTX, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Deletes dir location.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%-------------------------------------------------------------------
%% @doc
%% Returns dir_location document.
%% @end
%%-------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key)  ->
    datastore_model:get(?CTX, Key).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of storage_file_created field
%% @end
%%-------------------------------------------------------------------
-spec is_storage_file_created(doc() | dir_location() | undefined) -> boolean().
is_storage_file_created(undefined) ->
    false;
is_storage_file_created(#dir_location{storage_file_created = StorageFileCreated}) ->
    StorageFileCreated;
is_storage_file_created(#document{value = DirLocation}) ->
    is_storage_file_created(DirLocation).

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
        {storage_file_created, boolean}
    ]}.