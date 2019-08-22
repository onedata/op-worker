%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides fslogic access control list utility functions.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_logic).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([assert_permission_granted/4]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Traverses given ACL in order to check if a Principal (in our case GRUID),
%% has all permissions specified in 'OperationMask' (according to given ACL).
%% @end
%%--------------------------------------------------------------------
-spec assert_permission_granted(acl:acl(), od_user:doc(), non_neg_integer(),
    file_ctx:ctx()) -> ok | no_return().
assert_permission_granted(_Acl, _User, ?no_flags_mask, _FileCtx) ->
    ok;
assert_permission_granted([], _User, _OperationMask, _FileCtx) ->
    throw(?EACCES);
assert_permission_granted([Ace | Rest], User, OperationMask, FileCtx) ->
    {IsAceApplicable, FileCtx2} = check_ace_applicability(User, FileCtx, Ace),
    case IsAceApplicable of
        true ->
            case check_permission(OperationMask, Ace) of
                ok ->
                    ok;
                NewOpMask ->
                    assert_permission_granted(Rest, User, NewOpMask, FileCtx2)
            end;
        false ->
            assert_permission_granted(Rest, User, OperationMask, FileCtx2)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_ace_applicability(od_user:doc(), file_ctx:ctx(), acl:ace()) ->
    {boolean(), file_ctx:ctx()}.
check_ace_applicability(#document{key = UserId}, FileCtx, #access_control_entity{
    identifier = ?owner
}) ->
    {OwnerId, FileCtx2} = file_ctx:get_owner(FileCtx),
    {OwnerId == UserId, FileCtx2};

check_ace_applicability(#document{value = User}, FileCtx, #access_control_entity{
    identifier = ?group
}) ->
    {GroupOwnerId, FileCtx2} = file_ctx:get_group_owner(FileCtx),
    {lists:member(GroupOwnerId, User#od_user.eff_groups), FileCtx2};

check_ace_applicability(_UserDoc, FileCtx, #access_control_entity{
    identifier = ?everyone
}) ->
    {true, FileCtx};

check_ace_applicability(#document{value = User}, FileCtx, #access_control_entity{
    identifier = GroupId,
    aceflags = Flags
}) when ?has_flag(Flags, ?identifier_group_mask) ->
    {lists:member(GroupId, User#od_user.eff_groups), FileCtx};

check_ace_applicability(#document{key = UserId}, FileCtx, #access_control_entity{
    identifier = UserId
}) ->
    {true, FileCtx};

check_ace_applicability(_, FileCtx, _) ->
    {false, FileCtx}.


%% @private
-spec check_permission(non_neg_integer(), acl:ace()) ->
    ok | non_neg_integer() | no_return().
check_permission(Operations, #access_control_entity{
    acetype = ?allow_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        Operations -> ok;
        AllowedOperations -> Operations bxor AllowedOperations
    end;
check_permission(Operations, #access_control_entity{
    acetype = ?deny_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        ?no_flags_mask -> Operations;
        _ -> throw(?EACCES)
    end.
