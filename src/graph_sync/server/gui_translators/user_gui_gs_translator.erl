%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% user entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(user_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(GRI = #gri{aspect = instance, scope = private}, #od_user{
    full_name = FullName,
    username = Username
}) ->
    #{
        <<"fullName">> => FullName,
        <<"username">> => utils:undefined_to_null(Username),
        <<"spaceList">> => gri:serialize(GRI#gri{
            aspect = eff_spaces,
            scope = private
        })
    };
translate_resource(#gri{aspect = instance, scope = shared}, #{
    <<"fullName">> := FullName,
    <<"username">> := Username
}) ->
    #{
        <<"fullName">> => FullName,
        <<"username">> => utils:undefined_to_null(Username)
    };
translate_resource(#gri{aspect = eff_spaces, scope = private}, Spaces) ->
    #{
        <<"list">> => lists:map(fun(SpaceId) ->
            gri:serialize(#gri{
                type = op_space,
                id = SpaceId,
                aspect = instance,
                scope = private
            })
        end, Spaces)
    }.
