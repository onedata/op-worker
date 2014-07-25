%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jul 2014 23:56
%%%-------------------------------------------------------------------
-module(registry_spaces).
-author("RoXeon").

-include_lib("ctool/include/logging.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

%% API
-export([get_space_info/1]).


%% ====================================================================
%% API functions
%% ====================================================================


get_space_info(SpaceId) ->
    case global_registry:provider_request(get, "spaces/" ++ SpaceId) of
        {ok, Response} ->
            ?info("Resp: ~p", [Response]),
            #{<<"name">> := SpaceName} = Response,
            {ok, #space_info{uuid = SpaceId, name = binary_to_list(SpaceName)}};
        {error, Reason} ->
            {error, Reason}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

