%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides helper methods for processing requests that were
%%       rerouted to other provider.
%% @end
%% ===================================================================
-module(fslogic_remote).
-author("Rafal Slota").

-include("communication_protocol_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([postrouting/3, prerouting/3]).

%% ====================================================================
%% API functions
%% ====================================================================

prerouting(_, _, []) ->
    {error, no_providers};
prerouting(_SpaceInfo, #renamefile{from_file_logic_name = From, to_file_logic_name = To} = RequestBody, [RerouteTo | _Providers]) ->
    {ok, From1} = fslogic_path:get_full_file_name(From),
    {ok, To1} = fslogic_path:get_full_file_name(To),

    {ok, #space_info{providers = FromProviders}} = fslogic_utils:get_space_info_for_path(From1),
    {ok, #space_info{providers = ToProviders}} = fslogic_utils:get_space_info_for_path(To1),

    NotCommonProviders = FromProviders -- ToProviders,
    CommonProviders = FromProviders -- NotCommonProviders,

    case CommonProviders of
        [CommonProvider | _] ->
            {ok, {reroute, CommonProvider}};
        [] ->
            {ok, {reroute, RerouteTo}}
    end;
prerouting(_SpaceInfo, _Request, [RerouteTo | _Providers]) ->
    {ok, {reroute, RerouteTo}}.



%% postrouting/3
%% ====================================================================
%% @doc @todo: Write me !
%% @end
-spec postrouting(SpaceInfo :: #space_info{}, FailureReason :: term(), Request :: term()) -> Result :: undefined | term().
%% ====================================================================
postrouting(_SpaceInfo, {error, _FailureReason}, #getfilechildren{dir_logic_name = "/"}) ->
    #filechildren{answer = ?VOK, child_logic_name = [?SPACES_BASE_DIR_NAME]};
postrouting(#space_info{name = SpaceName} = SpaceInfo, {error, _FailureReason}, #getfileattr{file_logic_name = "/"}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
                links = 2, size = 0, type = ?DIR_TYPE, gname = unicode:characters_to_list(SpaceName), gid = fslogic_spaces:map_to_grp_owner(SpaceInfo),
                mode = ?SpaceDirPerm, uid = 0};
postrouting(#space_info{name = SpaceName} = SpaceInfo, {error, _FailureReason}, #getfileattr{file_logic_name = "/" ++ ?SPACES_BASE_DIR_NAME}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
        links = 2, size = 0, type = ?DIR_TYPE, gname = unicode:characters_to_list(SpaceName), gid = fslogic_spaces:map_to_grp_owner(SpaceInfo),
        mode = ?SpaceDirPerm, uid = 0};
postrouting(#space_info{}, {ok, #atom{value = ?VECOMM}}, #renamefile{} = RequestBody) ->
    fslogic:handle_fuse_message(RequestBody);
postrouting(#space_info{} = _SpaceInfo, {ok, Response}, _Request) ->
    Response;
postrouting(_SpaceInfo, UnkResult, Request) ->
    ?error("Unknown result ~p for request ~p", [UnkResult, Request]),
    undefined.
