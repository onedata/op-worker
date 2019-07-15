%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles translation of op logic results concerning
%%% space entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(space_rest_translator).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest.hrl").

-export([get_response/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(op_logic:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, SpaceData) ->
    ?OK_REPLY(SpaceData).
