%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions to add or strip names based on identifiers in ACLs.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_names).
-author("Lukasz Opiola").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([strip/1, add/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Removes name from access_control_entity record (sets its value to undefined).
%% @end
%%--------------------------------------------------------------------
-spec strip(Acl :: [#access_control_entity{}]) -> [#access_control_entity{}].
strip(Acl) ->
    lists:map(
        fun(#access_control_entity{} = Ace) ->
            Ace#access_control_entity{name = undefined}
        end, Acl).


%%--------------------------------------------------------------------
%% @doc
%% Resolves name for given access_control_entity record based on identifier
%% value and identifier type (user or group).
%% @end
%%--------------------------------------------------------------------
-spec add(Acl :: [#access_control_entity{}]) -> [#access_control_entity{}].
add(Acl) ->
    lists:map(
        fun(#access_control_entity{identifier = Id, aceflags = Flags} = Ace) ->
            Name = case ?has_flag(Flags, ?identifier_group_mask) of
                true -> gid_to_ace_name(Id);
                false -> uid_to_ace_name(Id)
            end,
            Ace#access_control_entity{name = Name}
        end, Acl).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Transforms global id to acl name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "John Dow#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec uid_to_ace_name(od_space:id()) -> binary().
uid_to_ace_name(?owner) ->
    undefined;
uid_to_ace_name(?everyone) ->
    undefined;
uid_to_ace_name(?group) ->
    undefined;
uid_to_ace_name(Uid) ->
    {ok, Name} = user_logic:get_name(?ROOT_SESS_ID, Uid),
    Name.

%%--------------------------------------------------------------------
%% @private
%% @doc Transforms global group id to acl group name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "group1#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec gid_to_ace_name(GroupId :: binary()) -> binary().
gid_to_ace_name(GroupId) ->
    {ok, Name} = group_logic:get_name(?ROOT_SESS_ID, GroupId),
    Name.