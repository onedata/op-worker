%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_user records
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

-type full_name() :: binary().
%% Linked account is expressed in the form of map:
%% #{
%%     <<"idp">> => binary(),
%%     <<"subjectId">> => binary(),
%%     <<"name">> => undefined | binary(),
%%     <<"login">> => undefined | binary(),
%%     <<"emails">> => [binary()],
%%     <<"entitlements">> => [binary()],
%%     <<"custom">> => jiffy:json_value()
%% }
-type linked_account() :: map().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([full_name/0, linked_account/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_posthooks/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update_cache(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update_cache(Id, Diff, Default) ->
    datastore_model:update(?CTX, Id, Diff, Default).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    datastore_model:delete(?CTX, Key).


-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).


%%--------------------------------------------------------------------
%% @doc
%% User create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, Doc}) ->
    run_after(Doc);
run_after(update, _, {ok, Doc}) ->
    run_after(Doc);
run_after(_Function, _Args, Result) ->
    Result.

-spec run_after(doc()) -> {ok, doc()}.
run_after(Doc = #document{key = UserId, value = #od_user{eff_spaces = EffSpaces}}) ->
    ok = file_meta:setup_onedata_user(UserId, EffSpaces),
    ok = permissions_cache:invalidate(),
    ok = files_to_chown:chown_pending_files(UserId),
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
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    6.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

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
    {record, lists:keyreplace(login, 1, Struct, {alias, string})};
get_record_struct(4) ->
    {record, Struct} = get_record_struct(3),
    % Rename email_list to emails
    {record, lists:keyreplace(email_list, 1, Struct, {emails, [string]})};
get_record_struct(5) ->
    % Rename name -> full_name
    % Rename alias -> username
    {record, [
        {full_name, string},
        {username, string},
        {emails, [string]},
        {linked_accounts, [ #{string => term} ]},

        {default_space, string},
        {space_aliases, #{string => string}},

        {eff_groups, [string]},
        {eff_spaces, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},

        {cache_state, #{atom => term}}
    ]};
get_record_struct(6) ->
    % Removed default_space field
    {record, [
        {full_name, string},
        {username, string},
        {emails, [string]},
        {linked_accounts, [ #{string => term} ]},

        {space_aliases, #{string => string}},

        {eff_groups, [string]},
        {eff_spaces, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},

        {cache_state, #{atom => term}}
    ]}.


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

        CacheState
    } = User,

    {3, {od_user,
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

        CacheState
    }};
upgrade_record(3, User) ->
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

        CacheState
    } = User,

    {4, {od_user,
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

        CacheState
    }};
upgrade_record(4, User) ->
    {od_user,
        Name,
        Alias,
        Emails,
        LinkedAccounts,

        DefaultSpace,
        SpaceAliases,

        EffGroups,
        EffSpaces,
        EffHandleServices,
        EffHandles,

        CacheState
    } = User,

    {5, {od_user,
        Name,
        Alias,
        Emails,
        LinkedAccounts,

        DefaultSpace,
        SpaceAliases,

        EffGroups,
        EffSpaces,
        EffHandleServices,
        EffHandles,

        CacheState
    }};
upgrade_record(5, User) ->
    {od_user,
        Name,
        Alias,
        Emails,
        LinkedAccounts,
        _DefaultSpace,

        SpaceAliases,

        EffGroups,
        EffSpaces,
        EffHandleServices,
        EffHandles,

        CacheState
    } = User,

    {6, #od_user{
        full_name = Name,
        username = Alias,
        emails = Emails,
        linked_accounts = LinkedAccounts,

        space_aliases = SpaceAliases,

        eff_groups = EffGroups,
        eff_spaces = EffSpaces,
        eff_handle_services = EffHandleServices,
        eff_handles = EffHandles,

        cache_state = CacheState
    }}.
