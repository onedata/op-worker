%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides fslogic access control list utility
%%% functions
%%% @end
%%%--------------------------------------------------------------------
-module(acl_logic).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_permission/4]).

-type ace() :: #access_control_entity{}.
-type acl() :: #acl{}.

-export_type([ace/0, acl/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Traverses given ACL in order to check if a Principal (in our case GRUID),
%% has permissions specified in 'OperationMask' (according to this ACL)
%% @end
%%--------------------------------------------------------------------
-spec check_permission([ace()], od_user:doc(), non_neg_integer(),
    file_ctx:ctx()) -> ok | no_return().
check_permission([], _User, ?no_flags_mask, _FileCtx) -> ok;
check_permission([], _User, _OperationMask, _FileCtx) -> throw(?EACCES);
check_permission([ACE = #access_control_entity{
    identifier = ?owner
} | Rest],
    User, Operation, FileCtx
) ->
    {OwnerId, FileCtx2} = file_ctx:get_owner(FileCtx),
    check_permission([ACE#access_control_entity{identifier = OwnerId} | Rest],
        User, Operation, FileCtx2);
check_permission([ACE = #access_control_entity{
    identifier = ?group,
    aceflags = Flags
} | Rest],
    User, Operation, FileCtx
) ->
    {GroupOwnerId, FileCtx2} = file_ctx:get_group_owner(FileCtx),
    check_permission([ACE#access_control_entity{
        identifier = GroupOwnerId,
        aceflags = Flags bor ?identifier_group_mask
    } | Rest], User, Operation, FileCtx2);
check_permission([ACE = #access_control_entity{
    identifier = ?everyone
} | Rest],
    User = #document{key = UserId}, Operation, FileCtx
) ->
    check_permission([ACE#access_control_entity{identifier = UserId} | Rest],
        User, Operation, FileCtx);
check_permission([#access_control_entity{
    acetype = Type,
    identifier = GroupId,
    aceflags = Flags,
    acemask = AceMask
} | Rest],
    User = #document{value = #od_user{eff_groups = Groups}},
    Operation, FileCtx) when ?has_flag(Flags, ?identifier_group_mask) ->

    case is_list(Groups) andalso lists:member(GroupId, Groups) of
        false ->
            check_permission(Rest, User, Operation, FileCtx); % if no group matches, ignore this ace
        true -> case Type of
            ?allow_mask ->
                case (Operation band AceMask) of
                    Operation -> ok;
                    OtherAllowedBits ->
                        check_permission(Rest, User, Operation bxor OtherAllowedBits, FileCtx)
                end;
            ?deny_mask ->
                case (Operation band AceMask) of
                    ?no_flags_mask ->
                        check_permission(Rest, User, Operation, FileCtx);
                    _ -> throw(?EACCES)
                end
        end
    end;
check_permission([#access_control_entity{
    acetype = ?allow_mask,
    identifier = SameUserId,
    acemask = AceMask
} | Rest],
    User = #document{key = SameUserId},
    Operation, FileCtx
) ->

    case (Operation band AceMask) of
        Operation -> ok;
        OtherAllowedBits ->
            check_permission(Rest, User, Operation bxor OtherAllowedBits, FileCtx)
    end;
check_permission([#access_control_entity{
    acetype = ?deny_mask,
    identifier = SameUserId,
    acemask = AceMask
} | Rest],
    User = #document{key = SameUserId},
    Operation, FileCtx
) ->
    case (Operation band AceMask) of
        ?no_flags_mask -> check_permission(Rest, User, Operation, FileCtx);
        _ -> throw(?EACCES)
    end;
check_permission([#access_control_entity{} | Rest], User = #document{}, Operation, FileCtx) ->
    check_permission(Rest, User, Operation, FileCtx).
