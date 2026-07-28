%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for setting up LUMA DB feeds in onenv tests.
%%%
%%% Local feed population goes through the same rpc_api functions that back
%%% the onepanel LUMA REST endpoints, i.e. the product path for defining
%%% local feed mappings. The feed data map follows the same schema as
%%% luma_test_server (see its module doc) so that a storage's mappings can be
%%% expressed identically regardless of the feed type under test.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_luma_test_utils).
-author("Bartosz Walkowicz").

%% API
-export([populate_local_feed/3]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec populate_local_feed(oct_background:node_selector(), binary(), map()) -> ok.
populate_local_feed(ProviderSelector, StorageId, FeedData) ->
    maps:foreach(fun(UserId, StorageUser) ->
        {ok, _} = opw_test_rpc:call(
            ProviderSelector, rpc_api, luma_storage_users_store, [StorageId, UserId, StorageUser]
        )
    end, maps:get(<<"storageUsers">>, FeedData, #{})),

    maps:foreach(fun(SpaceId, SpaceDefaults) ->
        % only the explicitly specified kinds of defaults are stored - not every kind
        % can be defined for every storage (e.g. posix storage defaults are rejected
        % for non posix compatible storages and for imported ones)
        maps:foreach(fun
            (<<"posix">>, PosixDefaults) ->
                ok = opw_test_rpc:call(ProviderSelector, rpc_api, luma_spaces_posix_storage_defaults_store, [
                    StorageId, SpaceId, PosixDefaults
                ]);
            (<<"display">>, DisplayDefaults) ->
                ok = opw_test_rpc:call(ProviderSelector, rpc_api, luma_spaces_display_defaults_store, [
                    StorageId, SpaceId, DisplayDefaults
                ])
        end, SpaceDefaults)
    end, maps:get(<<"spacesDefaults">>, FeedData, #{})),

    OnedataUsers = maps:get(<<"onedataUsers">>, FeedData, #{}),
    maps:foreach(fun(Uid, OnedataUser) ->
        ok = opw_test_rpc:call(ProviderSelector, rpc_api, luma_onedata_users_store_by_uid, [
            StorageId, Uid, OnedataUser
        ])
    end, maps:get(<<"uids">>, OnedataUsers, #{})),
    maps:foreach(fun(AclUser, OnedataUser) ->
        ok = opw_test_rpc:call(ProviderSelector, rpc_api, luma_onedata_users_store_by_acl_user, [
            StorageId, AclUser, OnedataUser
        ])
    end, maps:get(<<"aclUsers">>, OnedataUsers, #{})),

    maps:foreach(fun(AclGroup, OnedataGroup) ->
        ok = opw_test_rpc:call(ProviderSelector, rpc_api, luma_onedata_groups_store, [
            StorageId, AclGroup, OnedataGroup
        ])
    end, maps:get(<<"onedataGroups">>, FeedData, #{})),

    ok.
