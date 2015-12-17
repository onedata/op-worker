%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Security rules
%%% @end
%%%--------------------------------------------------------------------
-module(rules).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([check/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user.
%% @end
%%--------------------------------------------------------------------
-spec check(check_permissions:access_definition(), onedata_user:id()) -> term().
check(root, ?ROOT_USER_ID) ->
    ok;
check({owner, #document{value = #file_meta{uid = OwnerId}}}, OwnerId) ->
    ok;
check({validate_ancestors_exec, Doc}, UserId) ->
    ok = validate_ancestors_exec(Doc, UserId);
check({owner_if_parent_sticky, Doc}, UserId) ->
    #document{value = #file_meta{mode = Mode}} = fslogic_utils:get_parent(Doc),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            check({owner, Doc}, UserId);
        false ->
            ok
    end;
check({AccessType, #document{value = #file_meta{is_scope = true}} = Doc}, UserId) ->
    ok = validate_scope_access(AccessType, Doc, UserId),
    ok = validate_posix_access(AccessType, Doc, UserId);
check({AccessType, Doc}, UserId) ->
    ok = validate_posix_access(AccessType, Doc, UserId);
check(_, _) ->
    throw(?EPERM).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks whether given user has given permission on given file (POSIX permission check).
%%--------------------------------------------------------------------
-spec validate_posix_access(AccessType :: check_permissions:access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId) ->
    ReqBit =
        case AccessType of
            rdwr -> 8#6;
            read -> 8#4;
            write -> 8#2;
            exec -> 8#1
        end,

    IsAccessable =
        case UserId of
            OwnerId ->
                ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as owner.", [UserId, ReqBit, FileDoc, Mode]),
                ((ReqBit bsl 6) band Mode) =:= (ReqBit bsl 6);
            _ ->
                {ok, #document{value = #onedata_user{space_ids = Spaces}}} = onedata_user:get(UserId),
                {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
                case lists:member(fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID), Spaces) of
                    true ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as space member.", [UserId, ReqBit, FileDoc, Mode]),
                        ((ReqBit bsl 3) band Mode) =:= (ReqBit bsl 3);
                    false ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as other (Spaces ~p, scope ~p).", [UserId, ReqBit, FileDoc, Mode, Spaces, ScopeUUID]),
                        (ReqBit band Mode) =:= ReqBit
                end
        end,

    case IsAccessable of
        true -> ok;
        false -> throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc Checks whether given user has execute permission on all file's ancestors.
%%--------------------------------------------------------------------
-spec validate_ancestors_exec(Subj :: fslogic_worker:file(), UserId :: onedata_user:id()) -> ok | no_return().
validate_ancestors_exec(Subj, UserId) ->
    {ok, #document{value = #file_meta{is_scope = IsScope}} = SubjDoc} = file_meta:get(Subj),
    {ok, AncestorsIds} = file_meta:get_ancestors(SubjDoc),
    case IsScope of
        true ->
            ok = validate_posix_access(exec, SubjDoc, UserId);
        false ->
            ok
    end,
    lists:map(
        fun(AncestorId) ->
            #document{value = #file_meta{}} = FileDoc = check_permissions:get_validation_subject(UserId, {uuid, AncestorId}),
            ok = validate_posix_access(exec, FileDoc, UserId)
        end, AncestorsIds),
    ok.

%%--------------------------------------------------------------------
%% @doc Checks whether given user has given permission on scope given file.
%%      This function is always called before validate_posix_access/3 and shall handle all special cases.
%% @todo: Implement this method. Currently expected behaviour is to throw ENOENT instead EACCES for all spaces dirs.
%%--------------------------------------------------------------------
-spec validate_scope_access(AccessType :: check_permissions:access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_scope_access(_AccessType, _FileDoc, _UserId) ->
    ok.
