%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Security rules.
%%% @end
%%%--------------------------------------------------------------------
-module(rules).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([check_normal_or_default_def/3]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Checks if given access_definition is granted to given user.
%% Accepts default defs (that use default file context).
%% Returns updated default file context record.
%% @end
%%--------------------------------------------------------------------
-spec check_normal_or_default_def(check_permissions:access_definition(), user_ctx:ctx(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
check_normal_or_default_def({Type, FileCtx}, UserCtx, FileCtx) ->
    {ok, _FileCtx2} = check_normal_def({Type, FileCtx}, UserCtx, FileCtx);
check_normal_or_default_def({Type, SubjectCtx}, UserCtx, FileCtx) ->
    {ok, _} = check_normal_def({Type, SubjectCtx}, UserCtx, FileCtx),
    {ok, FileCtx};
check_normal_or_default_def(Type, UserCtx, FileCtx) ->
    {ok, _FileCtx2} = check_normal_def({Type, FileCtx}, UserCtx, FileCtx).


%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user. Accepts only full
%% definitions with file context. Return updated file context. The functions
%% gets as an argument also DefaultFileCtx, 'share' check depends on its
%% 'share_id'.
%% @end
%%--------------------------------------------------------------------
-spec check_normal_def(check_permissions:access_definition(),  user_ctx:ctx(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
check_normal_def({share, SubjectCtx}, UserCtx, DefaultFileCtx) ->
    case file_ctx:is_root_dir_const(SubjectCtx) of
        true ->
            throw(?EACCES);
        false ->
            {#document{value = #file_meta{shares = Shares}}, SubjectCtx2} =
                file_ctx:get_file_doc_including_deleted(SubjectCtx),
            ShareId = file_ctx:get_share_id_const(DefaultFileCtx),

            case lists:member(ShareId, Shares) of
                true ->
                    {ok, SubjectCtx2};
                false ->
                    {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, UserCtx),
                    rules_cache:check_and_cache_results([{share, ParentCtx}], UserCtx, DefaultFileCtx),
                    {ok, SubjectCtx3}
            end
    end;
check_normal_def({traverse_ancestors, SubjectCtx}, UserCtx, _DefaultFileCtx) ->
    case file_ctx:is_root_dir_const(SubjectCtx) of
        true ->
            {ok, SubjectCtx};
        false ->
            SubjectCtx2 = case file_ctx:is_space_dir_const(SubjectCtx) of
                true ->
                    {ok, SubjectCtx2_} = check_normal_def({?traverse_container, SubjectCtx}, UserCtx, _DefaultFileCtx),
                    SubjectCtx2_;
                false ->
                    SubjectCtx
            end,
            {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, UserCtx),
            rules_cache:check_and_cache_results([{?traverse_container, ParentCtx}], UserCtx, _DefaultFileCtx),
            rules_cache:check_and_cache_results([{traverse_ancestors, ParentCtx}], UserCtx, _DefaultFileCtx),
            {ok, SubjectCtx3}
    end;
check_normal_def({Type, SubjectCtx}, UserCtx, _FileCtx) ->
    {FileDoc, SubjectCtx2} = file_ctx:get_file_doc_including_deleted(SubjectCtx),
    ShareId = file_ctx:get_share_id_const(SubjectCtx2),
    case file_ctx:get_active_perms_type(SubjectCtx2) of
        {posix, SubjectCtx3} ->
            check(Type, FileDoc, UserCtx, ShareId, undefined, SubjectCtx3);
        {acl, SubjectCtx3} ->
            {Acl, _} = file_ctx:get_acl(SubjectCtx3),
            check(Type, FileDoc, UserCtx, ShareId, Acl, SubjectCtx3)
    end.


%%%===================================================================
%%% Internal Functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user.
%% @end
%%--------------------------------------------------------------------
-spec check(check_permissions:access_definition(), file_meta:doc(), user_ctx:ctx(),
    od_share:id() | undefined, acl:acl() | undefined, file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
% standard posix checks
check(root, _, _, _, _, _) ->
    throw(?EACCES);
check(owner, #document{value = #file_meta{owner = OwnerId}}, UserCtx, _, _, FileCtx) ->
    case user_ctx:get_user_id(UserCtx) =:= OwnerId of
        true ->
            {ok, FileCtx};
        false ->
            throw(?EACCES)
    end;
check(owner_if_parent_sticky, Doc, UserCtx, ShareId, Acl, FileCtx) ->
    {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    {#document{value = #file_meta{mode = Mode}}, _ParentCtx2} =
        file_ctx:get_file_doc_including_deleted(ParentCtx),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            check(owner, Doc, UserCtx, ShareId, Acl, FileCtx2);
        false ->
            {ok, FileCtx2}
    end;
check({AccessType1, 'or', AccessType2}, Doc, UserCtx, ShareId, Acl, FileCtx) ->
    case
        {
                catch check(AccessType1, Doc, UserCtx, ShareId, Acl, FileCtx),
                catch check(AccessType2, Doc, UserCtx, ShareId, Acl, FileCtx)
        }
    of
        {{ok, Ctx}, _} -> {ok, Ctx};
        {_, {ok, Ctx}} -> {ok, Ctx};
        _ -> throw(?EACCES)
    end;
check(AccessType, #document{value = #file_meta{is_scope = true}},
    UserCtx, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr
    ->
    {ok, FileCtx2} = validate_scope_access(FileCtx, UserCtx, ShareId),
    {ok, FileCtx3} = validate_posix_access(AccessType, FileCtx2, UserCtx, ShareId),
    validate_scope_privs(AccessType, FileCtx3, UserCtx, ShareId);
check(AccessType, _Doc, UserCtx, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    {ok, FileCtx2} = validate_posix_access(AccessType, FileCtx, UserCtx, ShareId),
    validate_scope_privs(AccessType, FileCtx2, UserCtx, ShareId);

% if no acl specified, map access masks checks to posix checks
check(?read_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?list_container, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?write_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?add_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?add_subcontainer, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_metadata, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?write_metadata, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?traverse_container, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?delete_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?delete_subcontainer, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?read_attributes, _Doc, _UserCtx, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_attributes, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?delete, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(owner_if_parent_sticky, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_acl, _Doc, _UserCtx, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_acl, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(owner, Doc, UserCtx, ShareId, undefined, FileCtx);

% acl is specified, check access masks
check(?read_object, _, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_object_mask, FileCtx
    ),
    validate_scope_privs(read, FileCtx, UserCtx, ShareId);
check(?list_container, _, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?list_container_mask, FileCtx
    ),
    validate_scope_privs(read, FileCtx, UserCtx, ShareId);
check(?write_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_object_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?add_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_object_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?add_subcontainer, _, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_subcontainer_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?read_metadata, _, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_metadata_mask, FileCtx
    ),
    validate_scope_privs(read, FileCtx, UserCtx, ShareId);
check(?write_metadata, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_metadata_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?traverse_container, #document{value = #file_meta{is_scope = true}},
    UserCtx, ShareId, Acl, FileCtx
) ->
    validate_scope_access(FileCtx, UserCtx, ShareId),
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?traverse_container_mask, FileCtx
    ),
    {ok, FileCtx};
check(?traverse_container, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?traverse_container_mask, FileCtx
    ),
    {ok, FileCtx};
check(?delete_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?delete_subcontainer, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?read_attributes, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_attributes_mask, FileCtx
    ),
    {ok, FileCtx};
check(?write_attributes, _, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_attributes_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?delete, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?read_acl, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_acl_mask, FileCtx
    ),
    {ok, FileCtx};
check(?write_acl, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_acl_mask, FileCtx
    ),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(Perm, File, User, ShareId, Acl, _FileCtx) ->
    ?error("Unknown permission check rule: (~p, ~p, ~p, ~p, ~p)",
        [Perm, File, User, ShareId, Acl]),
    throw(?EACCES).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has given permission on given file
%% (POSIX permission check).
%% @end
%%--------------------------------------------------------------------
-spec validate_posix_access(
    read | write | exec | rdwr, file_ctx:ctx(),
    user_ctx:ctx(), od_share:id() | undefined
) ->
    {ok, file_ctx:ctx()} | no_return().
validate_posix_access(rdwr, FileCtx, UserCtx, ShareId) ->
    {ok, FileCtx2} = validate_posix_access(write, FileCtx, UserCtx, ShareId),
    validate_posix_access(read, FileCtx2, UserCtx, ShareId);
validate_posix_access(AccessType, FileCtx, UserCtx, _ShareId) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        mode = Mode
    }}, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),

    ReqBit0 = case AccessType of
        read -> 8#4;
        write -> 8#2;
        exec -> 8#1
    end,

    ReqBit1 = case user_ctx:get_user_id(UserCtx) of
        OwnerId ->
            ReqBit0 bsl 6;  % shift to owner posix mode bits
        _ ->
            case file_ctx:is_in_user_space_const(FileCtx, UserCtx) of
                true ->
                    ReqBit0 bsl 3;  % shift to group posix mode bits
                false ->
                    ReqBit0     % remain at other posix mode bits
            end
    end,

    case ?has_flag(Mode, ReqBit1) of
        true ->
            {ok, FileCtx2};
        false ->
            throw(?EACCES)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to see given scope file.
%% This function is always called before validate_posix_access/3 and shall
%% handle all special cases.
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_access(file_ctx:ctx(), user_ctx:ctx(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_access(FileCtx, UserCtx, undefined) ->
    case user_ctx:is_guest(UserCtx)
        orelse file_ctx:is_in_user_space_const(FileCtx, UserCtx)
    of
        true ->
            {ok, FileCtx};
        false ->
            throw(?ENOENT)
    end;
validate_scope_access(FileCtx, _UserCtx, _ShareId) ->
    {ok, FileCtx}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to access given file with
%% respect to scope settings.
%% Exception to this are shared files which can be read without
%% SPACE_READ_DATA privilege.
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_privs(check_permissions:access_definition(), file_ctx:ctx(),
    user_ctx:ctx(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_privs(read, FileCtx, UserCtx, undefined) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {ok, FileCtx};
        false ->
            UserId = user_ctx:get_user_id(UserCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_READ_DATA) of
                true -> {ok, FileCtx};
                false -> throw(?EACCES)
            end
    end;
validate_scope_privs(write, FileCtx, UserCtx, _ShareId) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_WRITE_DATA) of
        true -> {ok, FileCtx};
        false -> throw(?EACCES)
    end;
validate_scope_privs(_, FileCtx, _, _) ->
    {ok, FileCtx}.
