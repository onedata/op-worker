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
-include("veil_modules/dao/dao.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

%% API
-export([local_clean_postprocess/3]).

%% ====================================================================
%% API functions
%% ====================================================================


%% local_clean_postprocess/3
%% ====================================================================
%% @doc @todo: Write me !
%% @end
-spec local_clean_postprocess(SpaceInfo :: #space_info{}, FailureReason :: term(), Request :: term()) -> Result :: undefined | term().
%% ====================================================================
local_clean_postprocess(_SpaceInfo, _FailureReason, #getfilechildren{dir_logic_name = "/"}) ->
    #filechildren{answer = ?VOK, child_logic_name = [?SPACES_BASE_DIR_NAME]};
local_clean_postprocess(#space_info{name = SpaceName} = SpaceInfo, _FailureReasen, #getfileattr{file_logic_name = "/"}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
                links = 2, size = 0, type = ?DIR_TYPE, gname = unicode:characters_to_list(SpaceName), gid = fslogic_spaces:map_to_grp_owner(SpaceInfo),
                mode = ?SpaceDirPerm, uid = 0};
local_clean_postprocess(#space_info{name = SpaceName} = SpaceInfo, _FailureReasen, #getfileattr{file_logic_name = "/" ++ ?SPACES_BASE_DIR_NAME}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
        links = 2, size = 0, type = ?DIR_TYPE, gname = unicode:characters_to_list(SpaceName), gid = fslogic_spaces:map_to_grp_owner(SpaceInfo),
        mode = ?SpaceDirPerm, uid = 0};
local_clean_postprocess(_, _, _) ->
    undefined.