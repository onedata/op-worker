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
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").


%% API
-export([local_clean_postprocess/2]).

local_clean_postprocess(_FailureReasen, #getfilechildren{dir_logic_name = "/"}) ->
    #filechildren{answer = ?VOK, child_logic_name = [?SPACES_BASE_DIR_NAME]};
local_clean_postprocess(_, _) ->
    undefined.