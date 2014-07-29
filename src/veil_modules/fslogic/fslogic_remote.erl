%%%-------------------------------------------------------------------
%%% @author rafal Slota
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jul 2014 22:31
%%%-------------------------------------------------------------------
-module(fslogic_remote).
-author("Rafal Slota").

-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").


%% API
-export([local_clean_postprocess/3]).

local_clean_postprocess(_SpaceInfo, _FailureReasen, #getfilechildren{dir_logic_name = "/"}) ->
    #filechildren{answer = ?VOK, child_logic_name = [?SPACES_BASE_DIR_NAME]};
local_clean_postprocess(#space_info{name = SpaceName} = SpaceInfo, _FailureReasen, #getfileattr{file_logic_name = "/"}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
                links = 2, size = 0, type = ?DIR_TYPE, gname = unicode:characters_to_list(SpaceName), gid = fslogic_spaces:map_to_grp_owner(SpaceInfo)};
local_clean_postprocess(_SpaceInfo, _FailureReasen, #getfileattr{file_logic_name = "/" ++ ?SPACES_BASE_DIR_NAME}) ->
    #fileattr{answer = ?VOK, atime = vcn_utils:time(), mtime = vcn_utils:time(), ctime = vcn_utils:time(),
        links = 2, size = 0, type = ?DIR_TYPE};
local_clean_postprocess(_, _, _) ->
    undefined.