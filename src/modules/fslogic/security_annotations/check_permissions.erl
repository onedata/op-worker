%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Tomasz Lichon
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
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {parent, item_definition()}.
-type check_type() :: owner % Check whether user owns the item
| traverse_ancestors % validates ancestors' exec permission.
| owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
| write | read | exec | rdwr
| acl_access_mask().
-type acl_access_mask() :: binary().

-type raw_access_definition() :: root | check_type() | {check_type(), item_definition()}.
-type access_definition() :: root | check_type() | {check_type(), file_ctx:ctx()}.

-export_type([check_type/0, item_definition/0, raw_access_definition/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% annotation's before_advice implementation.
%% #annotation.data type is [access_definition()] | access_definition()
%% @end
%%--------------------------------------------------------------------
-spec before_advice(#annotation{data :: [raw_access_definition()]}, module(), atom(), [term()]) ->
    [term()].
before_advice(#annotation{data = AccessDefinitions}, _M, _F, [#sfm_handle{
    session_id = SessionId,
    space_uuid = SpaceDirUuid,
    file_uuid = FileUuid
} | _] = Args
) ->
    UserCtx = user_ctx:new(SessionId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirUuid),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    DefaultFileCtx = file_ctx:new_by_guid(FileGuid), %todo store file_ctx in sfm_handle
    {ExpandedAccessDefinitions, DefaultFileCtx2} = expand_access_defs(AccessDefinitions, UserCtx, DefaultFileCtx, Args),
    lists:foreach(fun(Def) ->
        rules:check_normal_or_default_def(Def, UserCtx, DefaultFileCtx2) end, ExpandedAccessDefinitions),
    set_root_context_if_file_has_acl(Args);
before_advice(#annotation{data = AccessDefinitions}, _M, _F,
    Args = [UserCtx, DefaultFileCtx | OtherArgs]
) ->
    {ExpandedAccessDefinitions, DefaultFileCtx2} =
        expand_access_defs(AccessDefinitions, UserCtx, DefaultFileCtx, Args),
    lists:foreach(fun(Def) ->
        rules_cache:check_and_cache_result(Def, UserCtx, DefaultFileCtx2)
    end, ExpandedAccessDefinitions),
    case user_ctx:is_guest_context(UserCtx) of
        true ->
            rules_cache:check_and_cache_result(share, UserCtx, DefaultFileCtx2);
        false ->
            ok
    end,
    [UserCtx, DefaultFileCtx2 | OtherArgs].

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
%% @private
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_defs([raw_access_definition()], user_ctx:ctx(),
    file_ctx:ctx(), list()) -> {[access_definition()], file_ctx:ctx()}.
expand_access_defs(Defs, UserCtx, DefaultFileCtx, Args) ->
    case user_ctx:is_root_context(UserCtx) of
        true ->
            {[], DefaultFileCtx};
        false ->
            expand_access_defs_for_user(Defs, UserCtx, DefaultFileCtx, Args)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% Function returns also updated DefaultFileCtx record.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_defs_for_user([raw_access_definition()], user_ctx:ctx(),
    file_ctx:ctx(), list()) -> {[access_definition()], file_ctx:ctx()}.
expand_access_defs_for_user([], _UserCtx, DefaultFileCtx, _Inputs) ->
    {[], DefaultFileCtx};
expand_access_defs_for_user([root | _Rest], _UserCtx, _DefaultFileCtx, _Inputs) ->
    throw(?EACCES);
expand_access_defs_for_user([{CheckType, ItemDefinition} | Rest], UserCtx, DefaultFileCtx, Inputs) ->
    {FileCtx, DefaultFileCtx2} =
        resolve_file_entry(UserCtx, DefaultFileCtx, ItemDefinition, Inputs),
    {OtherAccesDefs, DefaultFileCtx3} =
        expand_access_defs_for_user(Rest, UserCtx, DefaultFileCtx2, Inputs),
    {[{CheckType, FileCtx} | OtherAccesDefs], DefaultFileCtx3};
expand_access_defs_for_user([CheckType | Rest], UserCtx, DefaultFileCtx, Inputs) ->
    {OtherAccesDefs, DefaultFileCtx2} =
        expand_access_defs_for_user(Rest, UserCtx, DefaultFileCtx, Inputs),
    {[CheckType | OtherAccesDefs], DefaultFileCtx2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts file() from argument list (Inputs) based on Item description.
%% @end
%%--------------------------------------------------------------------
-spec resolve_file_entry(user_ctx:ctx(), file_ctx:ctx(), item_definition(), [term()]) ->
    {file_ctx:ctx(), NewDefaultFileCtx :: file_ctx:ctx()}.
resolve_file_entry(_UserCtx, DefaultFileCtx, Item, Inputs) when is_integer(Item) ->
    {lists:nth(Item, Inputs), DefaultFileCtx};
resolve_file_entry(UserCtx, DefaultFileCtx, {parent, {path, Item}}, Inputs) when is_integer(Item) ->
    Path = lists:nth(Item, Inputs),
    ParentPath = filename:dirname(Path),
    {file_ctx:new_by_canonical_path(UserCtx, ParentPath), DefaultFileCtx};
resolve_file_entry(UserCtx, DefaultFileCtx, {path, Item}, Inputs) when is_integer(Item) ->
    Path = lists:nth(Item, Inputs),
    {file_ctx:new_by_canonical_path(UserCtx, Path), DefaultFileCtx};
resolve_file_entry(UserCtx, DefaultFileCtx, parent, _Inputs) ->
    {ParentCtx, DefaultFileCtx2} = file_ctx:get_parent(DefaultFileCtx, user_ctx:get_user_id(UserCtx)),
    {ParentCtx, DefaultFileCtx2};
resolve_file_entry(UserCtx, DefaultFileCtx, {parent, Item}, Inputs) ->
    {FileCtx, DefaultFileCtx2} = resolve_file_entry(UserCtx, DefaultFileCtx, Item, Inputs),
    {ParentCtx, _NewFileCtx} = file_ctx:get_parent(FileCtx, undefined),
    {ParentCtx, DefaultFileCtx2}.

%%--------------------------------------------------------------------
%% @private
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
