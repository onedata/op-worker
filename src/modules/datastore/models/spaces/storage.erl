%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for holding storage configuration.
%%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in next major release.
%%% @end
%%%-------------------------------------------------------------------
-module(storage).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([delete/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type record() :: #storage{}.
-type doc() :: datastore_doc:doc(record()).
-type name() :: binary().
-type helper() :: helpers:helper().

-export_type([record/0, doc/0, name/0, helper/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(od_storage:id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).


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
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    5.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, binary},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}}
        ]}]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean}
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(5) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean},
            {storage_path_type, string}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {api_key, string}
        ]}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {storage, Name, Helpers}) ->
    {2, {storage,
        Name,
        [{
            helper,
            helper:translate_name(HelperName),
            maps:fold(fun(K, V, Args) ->
                maps:put(helper:translate_arg_name(K), V, Args)
            end, #{}, HelperArgs),
            #{},
            false
        } || {_, HelperName, HelperArgs} <- Helpers],
        false
    }};
upgrade_record(2, {_, Name, Helpers, Readonly}) ->
    {3, {storage,
        Name,
        Helpers,
        Readonly,
        undefined
    }};
upgrade_record(3, {_, Name, Helpers, Readonly, LumaConfig}) ->
    {4, {storage,
        Name,
        [{
            helper,
            HelperName,
            HelperArgs,
            AdminCtx,
            Insecure,
            false
        } || {_, HelperName, HelperArgs, AdminCtx, Insecure} <- Helpers],
        Readonly,
        LumaConfig
    }};
upgrade_record(4, {_, Name, Helpers, Readonly, LumaConfig}) ->
    {5, {storage,
        Name,
        [
            #helper{
                name = HelperName,
                args = HelperArgs,
                admin_ctx = AdminCtx,
                insecure = Insecure,
                extended_direct_io = ExtendedDirectIO,
                storage_path_type = ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure,
            ExtendedDirectIO} <- Helpers
        ],
        Readonly,
        LumaConfig
    }}.

