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

update(_ProtocolVersion, Request) when is_record(Request, spacemodified) ->
  Id = Request#spacemodified.id,
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
  Id = Request#spaceremoved.id,
  case dao_lib:apply(dao_vfs, remove_file, [{uuid, Id}], ProtocolVersion) of
    ok -> ok;
    Error ->
      ?error("Space remove error: ~p for request ~p", [Error, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, usermodified) ->
  try
    Id = Request#usermodified.id,
    Spaces = Request#usermodified.spaces,
    Groups = Request#usermodified.groups,

    {ok, UserDoc} = dao_lib:apply(dao_users, get_user, [{uuid, Id}], ProtocolVersion),
    UserRec = UserDoc#db_document.record,
    case dao_lib:apply(dao_users, save_user, [UserRec#user{spaces = Spaces, groups = Groups}], ProtocolVersion) of
      {ok, _} -> ok;
      Error ->
        ?error("User update error: ~p for request ~p", [Error, Request])
    end
  catch
    E1:E2 ->
      ?error("User update error: ~p:~p for request ~p", [E1, E2, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, userremoved) ->
  Id = Request#userremoved.id,
  case dao_lib:apply(dao_users, remove_user, [{uuid, Id}], ProtocolVersion) of
    ok -> ok;
    Error ->
      ?error("User remove error: ~p for request ~p", [Error, Request])
  end,
  ok;

update(ProtocolVersion, Request) when is_record(Request, groupmodified) ->
  try
    Id = Request#groupmodified.id,
    Name = Request#groupmodified.name,

    {ok, GroupDoc} = dao_lib:apply(dao_groups, get_group, [Id], ProtocolVersion),
    GroupRec = GroupDoc#db_document.record,
    case dao_lib:apply(dao_groups, save_group, [GroupRec#group_details{name = Name}], ProtocolVersion) of
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
  Id = Request#groupremoved.id,
  case dao_lib:apply(dao_groups, remove_group, [Id], ProtocolVersion) of
    ok -> ok;
    Error ->
      ?error("Group remove error: ~p for request ~p", [Error, Request])
  end,
  ok.