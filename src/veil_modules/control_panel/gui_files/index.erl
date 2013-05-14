%% ===================================================================
%% author Lukasz Opiola
%%
%% This file contains nitrogen website code
%% (should it be mentioned in docs ??)
%% ===================================================================

-module (index).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

%% Template points to the template file, which will be filled with content
main() ->
  #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "VeilFS homepage v. 0.00000000000001".

%% This will be placed in the template instead of [[[page:header()]]] tag
header() ->
  #panel
  {
    style="margin: 10px; background-color: #66CC33; color: white; font-weight: bold;",
    body="VeilFS - Control panel"
  }.


%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
  [
    #panel { style="margin: 30px;", body=[
      #h1 { text="Z tego nic nie będzie, choć powstanie." }
    ]}
  ].