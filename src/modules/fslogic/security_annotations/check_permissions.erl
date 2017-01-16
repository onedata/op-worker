%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% check_permissions annotation implementation.
%%% This annotation shall check whether annotation's caller has given
%%% permissions to file that is also somewhere within annotation's arguments.
%%% @end
%%%-------------------------------------------------------------------
-module(check_permissions).
-annotation('function').
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5, get_validation_subject/2]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {parent, item_definition()}.
-type check_type() :: owner % Check whether user owns the item
| traverse_ancestors % validates ancestors' exec permission.
| owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
| write | read | exec | rdwr
| acl_access_mask().
-type acl_access_mask() :: binary().
-type access_definition() :: root | {check_type(), item_definition()}.
-type item() :: file_ctx:ctx().

-export_type([check_type/0, item_definition/0, access_definition/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% annotation's before_advice implementation.
%% #annotation.data type is [access_definition()] | access_definition()
%% @end
%%--------------------------------------------------------------------
-spec before_advice(#annotation{data :: [access_definition()]}, module(), atom(), [term()]) -> term().
before_advice(#annotation{data = AccessDefinitions}, _M, _F,
    [#sfm_handle{session_id = SessionId} | _] = Args
) ->
    UserCtx = user_ctx:new(SessionId),
    ExpandedAccessDefinitions = expand_access_defs(AccessDefinitions, UserCtx, Args),
    lists:foreach(fun check_rule/1, ExpandedAccessDefinitions),
    set_root_context_if_file_has_acl(Args);
before_advice(#annotation{data = AccessDefinitions}, _M, _F, Args = [UserCtx | _]) ->
    ExpandedAccessDefinitions = expand_access_defs(AccessDefinitions, UserCtx, Args),
    lists:foreach(fun check_rule_and_cache_result/1, ExpandedAccessDefinitions),
    Args.

%%--------------------------------------------------------------------
%% @doc annotation's after_advice implementation.
%%--------------------------------------------------------------------
-spec after_advice(#annotation{}, atom(), atom(), [term()], term()) -> term().
after_advice(#annotation{}, _M, _F, _Inputs, Result) ->
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_defs([access_definition()], user_ctx:ctx(), list()) ->
    [{check_type(), undefined | datastore:document(), datastore:document(),
        od_share:id() | undefined, [#accesscontrolentity{}]}].
expand_access_defs(Defs, UserCtx, Args) ->
    case user_ctx:is_root_context(UserCtx) of
        true ->
            [];
        false ->
            expand_access_defs_for_user(Defs, UserCtx, Args)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_defs_for_user([access_definition()], user_ctx:ctx(), list()) ->
    [{check_type(), user_ctx:ctx(), file_ctx:ctx()}].
expand_access_defs_for_user([], _UserCtx, _Inputs) ->
    [];
expand_access_defs_for_user([root | _Rest], _UserCtx, _Inputs) ->
    throw(?EACCES);
expand_access_defs_for_user([{traverse_ancestors, ItemDefinition} | Rest], UserCtx, Inputs) ->
    Subject = (catch get_file(ItemDefinition, UserCtx, Inputs)),
    {SubjectCtx, ParentCtx} =
        case file_ctx:is_file_ctx_const(Subject) of
            true ->
                {ParentCtx_, FileCtx2} = file_ctx:get_parent(Subject, user_ctx:get_user_id(UserCtx)),
                {FileCtx2, ParentCtx_};
            _ ->
                ParentCtx_ = get_file({parent, ItemDefinition}, UserCtx, Inputs),
                {undefined, ParentCtx_}
        end, %todo try to remove this logic
    expand_traverse_ancestors_check(SubjectCtx, ParentCtx, UserCtx)
    ++ expand_access_defs_for_user(Rest, UserCtx, Inputs);
expand_access_defs_for_user([{CheckType, ItemDefinition} | Rest], UserCtx, Inputs) ->
    FileCtx = get_file(ItemDefinition, UserCtx, Inputs),
    case check_permission_in_cache(CheckType, UserCtx, FileCtx) of
        {ok, ok} ->
            expand_access_defs_for_user(Rest, UserCtx, Inputs);
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            [{CheckType, UserCtx, FileCtx} | expand_access_defs_for_user(Rest, UserCtx, Inputs)]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file that shall be the subject of permission validation instead given file.
%% @end
%%--------------------------------------------------------------------
-spec get_validation_subject(user_ctx:ctx(), fslogic_worker:ext_file() | file_ctx:ctx()) ->
    file_ctx:ctx() | no_return().
get_validation_subject(UserCtx, #sfm_handle{file_uuid = FileUuid}) ->
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid)),
    get_validation_subject(UserCtx, FileCtx);
get_validation_subject(UserCtx, {path, Path}) ->
    file_ctx:new_by_path(UserCtx, Path);
get_validation_subject(_UserCtx, FileCtx) ->
    FileCtx.

%%--------------------------------------------------------------------
%% @doc
%% Extracts file() from argument list (Inputs) based on Item description.
%% @end
%%--------------------------------------------------------------------
-spec resolve_file_entry(item_definition(), [term()]) -> fslogic_worker:file() | file_ctx:ctx().
resolve_file_entry(Item, Inputs) when is_integer(Item) ->
    lists:nth(Item, Inputs);
resolve_file_entry({path, Item}, Inputs) when is_integer(Item) ->
    {path, resolve_file_entry(Item, Inputs)};
resolve_file_entry({parent, Item}, Inputs) ->
    ResolvedEntry = resolve_file_entry(Item, Inputs),
    case file_ctx:is_file_ctx_const(ResolvedEntry) of
        true ->
            {Parent, _NewFileCtx} = file_ctx:get_parent(ResolvedEntry, undefined),
            Parent;
        false ->
            #document{key = FileUuid} = fslogic_utils:get_parent(ResolvedEntry),
            file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Expand traverse_ancestors check into list of traverse_container check for
%% each ancestor and subject document
%% @end
%%--------------------------------------------------------------------
-spec expand_traverse_ancestors_check(SubjectCtx :: file_ctx:ctx(),
    ParentCtx :: file_ctx:ctx(), UserCtx :: user_ctx:ctx()) ->
    [{check_type(), user_ctx:ctx(), file_ctx:ctx()}].
expand_traverse_ancestors_check(SubjectCtx, ParentCtx, UserCtx) ->
    SubjectCtx2 =
        case SubjectCtx =/= undefined andalso file_ctx:is_space_dir_const(SubjectCtx) of
            true ->
                SubjectCtx;
            false ->
                ParentCtx
        end,
    {SubjectCheck, SubjectCtx4} =
        case file_ctx:is_dir(SubjectCtx2) of
            {true, SubjectCtx3} ->
                case check_permission_in_cache(?traverse_container, UserCtx, SubjectCtx3) of
                    {ok, ok} ->
                        {[], SubjectCtx3};
                    {ok, ?EACCES} ->
                        throw(?EACCES);
                    _ ->
                        {[{?traverse_container, UserCtx, SubjectCtx3}], SubjectCtx3}
                end;
            _ ->
                []
        end,

    {AncestorsCheck, CacheUsed} = expand_ancestors_check(SubjectCtx4, [], UserCtx),
    case {file_ctx:get_share_id_const(SubjectCtx4), CacheUsed} of
        {undefined, _} ->
            SubjectCheck ++ AncestorsCheck;
        {_, true} ->
            SubjectCheck ++ AncestorsCheck;
        {ShareId, false} ->
            PotentialShares = [SubjectCtx4] ++ [FileCtx || {_Type, _UserCtx, FileCtx} <- AncestorsCheck],
            IsValidShare = lists:any(
                fun(Ctx) ->
                    {#document{value = #file_meta{shares = Shares}}, _} = file_ctx:get_file_doc(Ctx),
                    lists:member(ShareId, Shares)
                end, PotentialShares),
            case IsValidShare of
                true ->
                    SubjectCheck ++ AncestorsCheck;
                false ->
                    throw(?EACCES)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Expand traverse_ancestors check into list of traverse_container check for
%% each ancestor and subject document. Starts from doc parent.
%% @end
%%--------------------------------------------------------------------
-spec expand_ancestors_check(file_ctx:ctx(), Acc, user_ctx:ctx()) ->
    {Acc, CacheUsed :: boolean()} when
    Acc :: [{acl_access_mask(), user_ctx:ctx(), file_ctx:ctx()}].
expand_ancestors_check(FileCtx, Acc, UserCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            {Acc, false};
        false ->
            {ParentCtx, _FileCtx2} = file_ctx:get_parent(FileCtx, user_ctx:get_user_id(UserCtx)),
            case check_permission_in_cache(?traverse_container, UserCtx, ParentCtx) of
                {ok, ok} ->
                    {Acc, true};
                {ok, ?EACCES} ->
                    throw(?EACCES);
                _ ->
                    expand_ancestors_check(ParentCtx, [{?traverse_container, UserCtx, ParentCtx} | Acc], UserCtx)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get file_doc matching given item definition
%% @end
%%--------------------------------------------------------------------
-spec get_file(item_definition(), user_ctx:ctx(), list()) -> file_ctx:ctx().
get_file(ItemDefinition, UserCtx, FunctionInputs) ->
    get_validation_subject(UserCtx, resolve_file_entry(ItemDefinition, FunctionInputs)).

%%--------------------------------------------------------------------
%% @doc
%% Check rule, throws if the rule condition is not met.
%% @end
%%--------------------------------------------------------------------
-spec check_rule({term(), user_ctx:ctx(), file_ctx:ctx()}) -> ok.
check_rule({AccessType, UserCtx, FileCtx}) ->
    ok = rules:check(AccessType, UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Check rule and cache result
%% @end
%%--------------------------------------------------------------------
-spec check_rule_and_cache_result({term(), file_ctx:ctx(), user_ctx:ctx()}) -> ok.
check_rule_and_cache_result({AccessType, UserCtx, FileCtx}) ->
    try
        check_rule({AccessType, UserCtx, FileCtx}),
        cache_permission(AccessType, UserCtx, FileCtx, ok)
    catch
        _:?EACCES ->
            cache_permission(AccessType, UserCtx, FileCtx, ?EACCES),
            throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file associated with handle has acl defined. If so, function
%% updates storage handle context to ROOT in provided arguments.
%% @end
%%--------------------------------------------------------------------
-spec set_root_context_if_file_has_acl([#sfm_handle{} | term()]) ->
    [#sfm_handle{} | term()].
set_root_context_if_file_has_acl(Args = [Handle = #sfm_handle{file_uuid = FileUuid} | RestOfArgs]) ->
    case (catch acl:exists(FileUuid)) of
        true ->
            [Handle#sfm_handle{session_id = ?ROOT_SESS_ID} | RestOfArgs];
        _ ->
            Args
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns cached permission for given definition.
%% @end
%%--------------------------------------------------------------------
-spec check_permission_in_cache(check_type(), user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, term()} | calculate | no_return().
check_permission_in_cache(CheckType, UserCtx, FileCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    permissions_cache:check_permission({CheckType, UserId, Guid}).

%%--------------------------------------------------------------------
%% @doc
%% Caches result for given definition
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(check_type(), user_ctx:ctx(), file_ctx:ctx(), ok | ?EACCES) ->
    {ok, term()} | calculate | no_return().
cache_permission(CheckType, UserCtx, FileCtx, Value) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    permissions_cache:cache_permission({CheckType, UserId, Guid}, Value).