%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model server as cache for od_user records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_user).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type record() :: #od_user{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type name() :: binary().
%% Oauth linked accounts in form of map:
%% #{
%%     <<"providerId">> => binary(),
%%     <<"userId">> => binary(),
%%     <<"login">> => binary(),
%%     <<"name">> => binary(),
%%     <<"emailList">> => [binary()],
%%     <<"groups">> => [binary()]
%% }
-type linked_account() :: maps:map().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([name/0, linked_account/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%% API
-export([save/1, get/1, delete/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_posthooks/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves handle.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    case datastore_model:save(?CTX, Doc) of
        {ok, #document{key = UserId, value = #od_user{eff_spaces = EffSpaces}}} ->
            file_meta:setup_onedata_user(UserId, EffSpaces),
            {ok, UserId};
        Error ->
            Error
    end.

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
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% User create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(save, _, {ok, Doc = #document{key = UserId}}) ->
    ok = permissions_cache:invalidate(),
    ok = files_to_chown:chown_pending_files(UserId),
    {ok, Doc};
run_after(_Function, _Args, Result) ->
    Result.

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
    3.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun(Function, Args, Result) ->
        od_user:run_after(Function, Args, Result)
    end].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {alias, string},
        {email_list, [string]},
        {connected_accounts, [[{string, term}]]},

        {default_space, string},
        {space_aliases, [{string, string}]},

        {groups, [string]},
        {spaces, [string]},
        {handle_services, [string]},
        {handles, [string]},

        {eff_groups, [string]},
        {eff_spaces, [string]},
        {eff_shares, [string]},
        {eff_providers, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},

        {public_only, boolean},
        {revision_history, [term]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {login, string},
        {email_list, [string]},
        {linked_accounts, [ #{string => term} ]},

        {default_space, string},
        {space_aliases, #{string => string}},

        {eff_groups, [string]},
        {eff_spaces, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},

        {cache_state, #{atom => term}}
    ]};
get_record_struct(3) ->
    {record, Struct} = get_record_struct(2),
    % Rename login to alias
    {record, lists:keyreplace(login, 1, Struct, {alias, string})}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, User) ->
    {od_user,
        Name,
        Alias,
        EmailList,
        _ConnectedAccounts,

        _DefaultSpace,
        _SpaceAliases,

        _Groups,
        _Spaces,
        _HandleServices,
        _Handles,

        _EffGroups,
        _EffSpaces,
        _EffShares,
        _EffProviders,
        _EffHandleServices,
        _EffHandles,

        _PublicOnly,
        _RevisionHistory
    } = User,
    {2, {od_user,
        Name,
        Alias,
        EmailList,
        [#{}],

        undefined,
        #{},

        [],
        [],
        [],
        [],

        #{}
    }};
upgrade_record(2, User) ->
    {od_user,
        Name,
        Alias,
        EmailList,
        LinkedAccounts,

        DefaultSpace,
        SpaceAliases,

        EffGroups,
        EffSpaces,
        EffHandleServices,
        EffHandles,

        _CacheState
    } = User,

    {3, #od_user{
        name = Name,
        alias = Alias,
        email_list = EmailList,
        linked_accounts = LinkedAccounts,

        default_space = DefaultSpace,
        space_aliases = SpaceAliases,

        eff_groups = EffGroups,
        eff_spaces = EffSpaces,
        eff_handle_services = EffHandleServices,
        eff_handles = EffHandles,

        cache_state = #{}
    }}.
