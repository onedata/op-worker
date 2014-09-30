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
-include("oneprovider_modules/dao/dao.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
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
    {ok, {response, Response :: term()}} | {ok, {reroute, ProviderId :: binary(), NewRequest :: term()}} | {error, Reason :: any()}.
%% ====================================================================
prerouting(_, _, []) ->
    {error, no_providers};
prerouting(_SpaceInfo, #renamefile{from_file_logic_name = From, to_file_logic_name = To} = RequestBody, [RerouteTo | _Providers]) ->
    {ok, From1} = fslogic_path:get_full_file_name(From),
    {ok, To1} = fslogic_path:get_full_file_name(To),

    RequestBody1 = RequestBody#renamefile{from_file_logic_name = From1, to_file_logic_name = To1},

    {ok, #space_info{providers = FromProviders}} = fslogic_utils:get_space_info_for_path(From1),
    {ok, #space_info{providers = ToProviders}} = fslogic_utils:get_space_info_for_path(To1),

    FromProvidersSet = ordsets:from_list(FromProviders),
    ToProvidersSet = ordsets:from_list(ToProviders),
    CommonProvidersSet = ordsets:intersection(FromProvidersSet, ToProvidersSet),

    case ordsets:to_list(CommonProvidersSet) of
        [CommonProvider | _] ->
            {ok, {reroute, CommonProvider, RequestBody1}};
        [] ->
            {ok, {reroute, RerouteTo, RequestBody1}}
    end;
prerouting(_SpaceInfo, RequestBody, [RerouteTo | _Providers]) ->
    Path = fslogic:extract_logical_path(RequestBody),

    %% Replace all paths in this request with their "full" versions (with /spaces prefix).
    {ok, FullPath} = fslogic_path:get_full_file_name(Path),
    TupleList = lists:map(
        fun(Elem) ->
            case Elem of
                Path -> FullPath;
                _    -> Elem
            end
        end, tuple_to_list(RequestBody)),
    RequestBody1 = list_to_tuple(TupleList),
    {ok, {reroute, RerouteTo, RequestBody1}}.



%% postrouting/3
%% ====================================================================
%% @doc This function is called for each response from remote provider and allows altering this response
%%      (i.e. show empty directory instead of error in some cases).
%%      'undefined' return value means, that response is invalid and the whole rerouting process shall fail.
%% @end
-spec postrouting(SpaceInfo :: #space_info{}, {ok | error, ResponseOrReason :: term()}, Request :: term()) -> Result :: undefined | term().
%% ====================================================================
postrouting(_SpaceInfo, {error, _FailureReason}, #getfilechildren{dir_logic_name = "/", offset = 0}) ->
    #filechildren{answer = ?VOK, entry = [#filechildren_direntry{name = ?SPACES_BASE_DIR_NAME, type = ?DIR_TYPE_PROT}]};
postrouting(_SpaceInfo, {ok, #filechildren{answer = ?VOK, entry = Entries} = Response}, #getfilechildren{dir_logic_name = "/", offset = 0, children_num = Count}) ->
    Entries1 = [#filechildren_direntry{name = ?SPACES_BASE_DIR_NAME, type = ?DIR_TYPE_PROT} | Entries],
    Entries2 = lists:sublist(Entries1, Count),
    Response#filechildren{entry = Entries2};
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
postrouting(#space_info{}, {ok, #atom{value = ?VEACCES}}, #renamefile{} = RequestBody) ->
    fslogic:handle_fuse_message(RequestBody);
postrouting(#space_info{}, {ok, #atom{value = ?VEPERM}}, #renamefile{} = RequestBody) ->
    fslogic:handle_fuse_message(RequestBody);
postrouting(#space_info{} = _SpaceInfo, {ok, Response}, _Request) ->
    Response;
postrouting(_SpaceInfo, UnkResult, Request) ->
    ?error("Unknown result ~p for request ~p", [UnkResult, Request]),
    undefined.
