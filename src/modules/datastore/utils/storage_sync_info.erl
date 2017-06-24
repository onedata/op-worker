%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains helper functions for storage_sync_info structure
%%% which is member of file_meta record and is used by storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_info).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_specific_models_def.hrl").

%% API
-export([update/4, update/2]).


%%--------------------------------------------------------------------
%% @doc
%% @equiv update_storage_sync_info(Uuid, NewMTime, undefined, undefined)
%% @end
%%--------------------------------------------------------------------
-spec update(file_meta:uuid(), non_neg_integer()) ->
    {ok, file_meta:uuid()} | datastore:update_error().
update(Uuid, NewMTime) ->
    update(Uuid, NewMTime, undefined, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info field of #file_meta record.
%% @end
%%--------------------------------------------------------------------
-spec update(file_meta:uuid(), non_neg_integer(), non_neg_integer(),
    binary()) -> {ok, file_meta:uuid()} | datastore:update_error().
update(Uuid, NewMTime, NewHashKey, NewHashValue) ->
    file_meta:update({uuid, Uuid},
        fun
            (Value = #file_meta{
                storage_sync_info = #storage_sync_info{
                    last_synchronized_mtime = MTime0,
                    children_attrs_hashes = ChildrenAttrsHashes0
            }}) ->

                MTime = utils:ensure_defined(NewMTime, undefined, MTime0),
                ChildrenAttrsHashes = case NewHashKey of
                    undefined -> ChildrenAttrsHashes0;
                    _ ->
                        ChildrenAttrsHashes0#{NewHashKey => NewHashValue}
                end,

                {ok, Value#file_meta{
                    storage_sync_info = #storage_sync_info{
                        last_synchronized_mtime = MTime,
                        children_attrs_hashes = ChildrenAttrsHashes
                    }}}
        end).



