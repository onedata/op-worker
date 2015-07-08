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
-module(page_404).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
%% Include common gui hrl from ctool
-include_lib("ctool/include/gui/common.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> <<"error 404">>.

event(init) -> ok;
event(terminate) -> ok.
