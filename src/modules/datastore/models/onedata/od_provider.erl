%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_provider records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_provider).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type record() :: #od_provider{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type name() :: binary().
-type domain() :: domain().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([name/0, domain/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%% API
-export([save_to_cache/1, get_from_cache/1, invalidate_cache/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec save_to_cache(doc()) -> {ok, id()} | {error, term()}.
save_to_cache(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    datastore_model:delete(?CTX, Key).


-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

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
    4.

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
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {urls, [string]},

        {spaces, #{string => integer}},

        {eff_users, [string]},
        {eff_groups, [string]},

        {cache_state, #{atom => term}}
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},
        {admin_email, string},
        {subdomain_delegation, boolean},
        {domain, string},
        {subdomain, string},
        {latitude, float},
        {longitude, float},
        {online, boolean},

        {spaces, #{string => integer}},

        {eff_users, [string]},
        {eff_groups, [string]},

        {cache_state, #{atom => term}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},
        {admin_email, string},
        {subdomain_delegation, boolean},
        {domain, string},
        {subdomain, string},
        {latitude, float},
        {longitude, float},
        {online, boolean},

        {spaces, #{string => integer}},

        {eff_users, [string]},
        {eff_groups, [string]},
        {eff_peers, [string]}, % new field

        {cache_state, #{atom => term}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, Provider) ->
    {
        od_provider,
        Name,
        Urls,
        _Spaces,

        _PublicOnly,
        _RevisionHistory
    } = Provider,
    {2, {od_provider,
        Name,
        Urls,

        #{},

        [],
        [],

        #{}
    }};
upgrade_record(2, Provider) ->
    {
        od_provider,
        Name,
        Urls,

        #{},

        [],
        [],

        #{}
    } = Provider,
    #{host := Domain} = url_utils:parse(hd(Urls)),
    {3, {od_provider, 
        Name,
        undefined,
        false,
        Domain,
        undefined,
        0.0,
        0.0,
        false,

        #{},

        [],
        [],

        #{}
    }};
upgrade_record(3, Provider) ->
    {
        od_provider,
        Name,
        AdminEmail,
        SubdomainDelegation,
        Domain,
        Subdomain,
        Latitude,
        Longitude,
        Online,

        Spaces,

        EffUsers,
        EffGroups,

        CacheState
    } = Provider,
    {4, #od_provider{
        name = Name,
        admin_email = AdminEmail,
        subdomain_delegation = SubdomainDelegation,
        domain = Domain,
        subdomain = Subdomain,
        latitude = Latitude,
        longitude = Longitude,
        online = Online,

        spaces = Spaces,

        eff_users = EffUsers,
        eff_groups = EffGroups,
        eff_peers = [],

        cache_state = CacheState
    }}.
