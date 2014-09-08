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

%% prerouting/3
%% ====================================================================
%% @doc This function is called for each request that should be rerouted to remote provider and allows to choose
%%      the provider ({ok, {reroute, ProviderId}}), stop rerouting while giving response to the request ({ok, {response, Response}})
%%      or stop rerouting due to error.
%% @end
-spec prerouting(SpaceInfo :: #space_info{}, Request :: term(), [ProviderId :: binary()]) ->
    {ok, {response, Response :: term()}} | {ok, {reroute, ProviderId :: binary()}} | {error, Reason :: any()}.
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
%% @doc This function is called for each response from remote provider and allows altering this response
%%      (i.e. show empty directory instead of error in some cases).
%%      'undefined' return value means, that response is invalid and the whole rerouting process shall fail.
%% @end
-spec postrouting(SpaceInfo :: #space_info{}, {ok | error, ResponseOrReason :: term()}, Request :: term()) -> Result :: undefined | term().
%% ====================================================================
postrouting(_SpaceInfo, {error, _FailureReason}, #getfilechildren{dir_logic_name = "/"}) ->
    #filechildren{answer = ?VOK, entry = [#filechildren_direntry{name = ?SPACES_BASE_DIR_NAME, type = ?DIR_TYPE_PROT}]};
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
