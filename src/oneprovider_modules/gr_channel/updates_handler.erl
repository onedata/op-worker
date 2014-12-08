%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements communication with Global Registry logic
%% connected with metadata updates.
%% @end
%% ===================================================================
-module(updates_handler).

-include("gr_messages_pb.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/2]).

%% ===================================================================
%% API
%% ===================================================================

%% update/1
%% ====================================================================
%% @doc Update DB (triggered by GR requests).
%% @end
-spec update(ProtocolVersion :: non_neg_integer(), Request :: term()) -> ok.
%% ====================================================================
update(_ProtocolVersion, Request) when is_record(Request, spacemodified) ->
  Id = utils:ensure_binary(Request#spacemodified.id),
  Name = Request#spacemodified.name,
  SizeRecords = Request#spacemodified.size,
  Size = lists:map(fun(S) -> {S#spacemodified_size.provider, S#spacemodified_size.size} end, SizeRecords),
  Users = Request#spacemodified.users,
  Groups = Request#spacemodified.groups,
  Providers = Request#spacemodified.providers,

  SI = #space_info{space_id = Id, name = Name, providers = Providers, groups = Groups, users = Users, size = Size},
  fslogic_spaces:initialize(SI),
  ok;

update(ProtocolVersion, Request) when is_record(Request, spaceremoved) ->
  try
  Id = binary_to_list(Request#spaceremoved.id),
  {ok, SpaceDoc} = dao_lib:apply(dao_vfs, get_space_file, [{uuid, Id}], ProtocolVersion),
  ok = dao_lib:apply(dao_vfs, remove_file, [{uuid, SpaceDoc#db_document.uuid}], ProtocolVersion)
  catch
    E1:E2 ->
      ?error("Space remove error: ~p:~p for request ~p", [E1, E2, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, usermodified) ->
  try
    Id = binary_to_list(Request#usermodified.id),
    Spaces = Request#usermodified.spaces,
    Groups = Request#usermodified.groups,

    case dao_lib:apply(dao_users, get_user, [{uuid, Id}], ProtocolVersion) of
      {ok, UserDoc} ->
        UserRec = UserDoc#db_document.record,
        case dao_lib:apply(dao_users, save_user, [UserDoc#db_document{record = UserRec#user{spaces = Spaces, groups = Groups}}], ProtocolVersion) of
          {ok, _} -> ok;
          Error ->
            ?error("User update error: ~p for request ~p", [Error, Request])
        end;
      {error, {not_found, _}} ->
        {ok, _} = user_logic:create_partial_user(Id, Spaces, Groups);
      E ->
        throw({get_user_error, E})
    end
  catch
    E1:E2 ->
      ?error("User update error: ~p:~p for request ~p", [E1, E2, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, userremoved) ->
  Id = binary_to_list(Request#userremoved.id),
  case dao_lib:apply(dao_users, remove_user, [{uuid, Id}], ProtocolVersion) of
    ok -> ok;
    Error ->
      ?error("User remove error: ~p for request ~p", [Error, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, groupmodified) ->
  try
    Id = binary_to_list(Request#groupmodified.id),
    Name = Request#groupmodified.name,

    SaveArg = case dao_lib:apply(dao_groups, get_group, [Id], ProtocolVersion) of
      {ok, GroupDoc} ->
        GroupRec = GroupDoc#db_document.record,
        GroupDoc#db_document{record = GroupRec#group_details{name = Name}};
      {error, {not_found, _}} ->
        #group_details{id = Id, name = Name};
      E ->
        throw({get_group_error, E})
    end,
    case dao_lib:apply(dao_groups, save_group, [SaveArg], ProtocolVersion) of
      {ok, _} -> ok;
      Error ->
        ?error("Group update error: ~p for request ~p", [Error, Request])
    end
  catch
    E1:E2 ->
      ?error("Group update error: ~p:~p for request ~p", [E1, E2, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, groupremoved) ->
  Id = binary_to_list(Request#groupremoved.id),
  case dao_lib:apply(dao_groups, remove_group, [Id], ProtocolVersion) of
    ok -> ok;
    Error ->
      ?error("Group remove error: ~p for request ~p", [Error, Request])
  end,
  ok.