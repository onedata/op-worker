%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% group entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(group_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, id = GroupId, scope = private}, #od_group{
    name = Name,
    type = Type
}) ->
    #{
        <<"name">> => Name,
        <<"type">> => Type,
        <<"effUserList">> => gri:serialize(#gri{
            type = op_group,
            id = GroupId,
            aspect = eff_users,
            scope = private
        })
    };
translate_resource(#gri{aspect = instance, scope = shared}, GroupInfo) ->
    GroupInfo;

translate_resource(#gri{aspect = eff_users, scope = private}, UserIds) ->
    #{
        <<"list">> => lists:map(fun(UserId) ->
            gri:serialize(#gri{
                type = op_user,
                id = UserId,
                aspect = instance,
                scope = shared
            })
        end, UserIds)
    }.
