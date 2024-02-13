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
        <<"effSpaceList">> => gri:serialize(GRI#gri{
            aspect = eff_spaces,
            scope = private
        }),
        <<"effGroupList">> => gri:serialize(GRI#gri{
            aspect = eff_groups,
            scope = private
        }),
        <<"effHandleServiceList">> => gri:serialize(GRI#gri{
            aspect = eff_handle_services,
            scope = private
        }),
        <<"effAtmInventoryList">> => gri:serialize(GRI#gri{
            aspect = eff_atm_inventories,
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
    };
translate_resource(#gri{aspect = eff_groups, scope = private}, Groups) ->
    #{
        <<"list">> => lists:map(fun(SpaceId) ->
            gri:serialize(#gri{
                type = op_group,
                id = SpaceId,
                aspect = instance,
                scope = private
            })
        end, Groups)
    };
translate_resource(#gri{aspect = eff_handle_services, scope = private}, HServices) ->
    #{
        <<"list">> => lists:map(fun(HandleServiceId) ->
            gri:serialize(#gri{
                type = op_handle_service,
                id = HandleServiceId,
                aspect = instance,
                scope = private
            })
        end, HServices)
    };
translate_resource(#gri{aspect = eff_atm_inventories, scope = private}, AtmInventories) ->
    #{
        <<"list">> => lists:map(fun(AtmInventoryId) ->
            gri:serialize(#gri{
                type = op_atm_inventory,
                id = AtmInventoryId,
                aspect = instance,
                scope = private
            })
        end, AtmInventories)
    }.
