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
%%     <<"username">> => undefined | binary(),
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
    memory_copies => all,
    disc_driver => undefined
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_posthooks/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update_cache(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update_cache(Id, Diff, Default) ->
    run_in_critical_section(Id, fun() ->
        PrevVal = case get_from_cache(Id) of
            {ok, #document{value = V}} -> V;
            {error, not_found} -> #od_user{}
        end,
        case datastore_model:update(?CTX, Id, Diff, Default) of
            {ok, #document{value = NewVal}} = Res ->
                ok = handle_new_doc(Id, PrevVal, NewVal),
                ok = handle_new_spaces(Id, PrevVal, NewVal),
                ok = handle_spaces_removed(Id, PrevVal, NewVal),
                Res;
            {error, _} = Error ->
                Error
        end
    end).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    run_in_critical_section(Key, fun() ->
        datastore_model:delete(?CTX, Key)
    end).


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
run_after(Doc = #document{key = UserId, value = Record}) ->
    ok = permissions_cache:invalidate(),
    ok = files_to_chown:chown_deferred_files(UserId),
    case Record of
        % report a change in the access block only when a change in the current
        % value was observed
        #od_user{blocked = {Blocked, changed}} ->
            auth_cache:report_user_access_block_changed(UserId, Blocked);
        _ ->
            ok
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
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec run_in_critical_section(id(), fun (() -> Result)) -> Result.
run_in_critical_section(UserId, Fun) ->
    critical_section:run({od_user, UserId}, Fun).


%% @private
-spec handle_new_spaces(id(), PrevVal :: record(), NewVal :: record()) -> ok.
handle_new_spaces(UserId, #od_user{eff_spaces = PrevSpaces}, #od_user{eff_spaces = NewSpaces}) ->
    % NOTE: PrevVal is an empty record (see update_cache/3) if previous document does not exist
    case NewSpaces -- PrevSpaces of
        [] -> ok;
        SpacesDiff -> user_root_dir:report_new_spaces_appeared([UserId], SpacesDiff)
    end.


%% @private
-spec handle_spaces_removed(id(), PrevVal :: record(), NewVal :: record()) -> ok.
handle_spaces_removed(UserId, #od_user{eff_spaces = PrevSpaces}, #od_user{eff_spaces = NewSpaces}) ->
    % NOTE: PrevVal is an empty record (see update_cache/3) if previous document does not exist
    case PrevSpaces -- NewSpaces of
        [] -> ok;
        SpacesDiff -> user_root_dir:report_spaces_removed([UserId], SpacesDiff)
    end.


%% @private
-spec handle_new_doc(id(), PrevVal :: record(), NewVal :: record()) -> ok.
handle_new_doc(UserId, #od_user{username = undefined}, _) ->
    % NOTE: PrevVal is an empty record (see update_cache/3) if previous document does not exist
    user_root_dir:ensure_docs_exist(UserId);
handle_new_doc(_, _, _) ->
    ok.
