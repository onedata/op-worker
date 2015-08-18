%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file contains n2o website code.
%%% The page is displayed when client asks for not existing resource.
%%% @end
%%%--------------------------------------------------------------------
-module(page_ember).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
%% Include common gui hrl from ctool
-include_lib("ctool/include/gui/common.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API
-export([main/0, event/1, api_event/3]).

%% Template points to the template file, which will be filled with content
main() ->
    gui_jq:wire(#api{name = "api_fun", tag = "api_fun"}),
    #dtl{file = "page_404", app = ?APP_NAME}.

api_event("api_fun", _, _) ->
    random:seed(now()),
    N = random:uniform(234234),
    NewStr = <<"Oto randomowa liczba: ", (integer_to_binary(N))/binary>>,
%%     ?dump(NewStr),
    gui_jq:wire(<<"show_alert(' brakuje spacji');">>),
    gui_jq:update(<<"message">>, NewStr).

event(init) ->
    ok;
event(terminate) -> ok.
