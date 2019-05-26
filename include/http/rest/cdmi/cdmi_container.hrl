%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header provides common definitions for cdmi_container objects,
%%% (they can be accessed via cdmi_container.erl handler)
%%% @end
%%%-------------------------------------------------------------------


%% the default json response for container object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the requested url
-define(default_get_dir_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"childrenrange">>, <<"children">>]).
