%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provies routing from cdmi_handler to cdmi submodules.
%% @end
%% ===================================================================
-module(cdmi_routes).

%% API
-export([route/2]).

%% route/2
%% ====================================================================
%% @doc
%% This function returns cdmi handler module.
%% The first argument is a list of binaries - result of splitting request subpath on "/".
%% Subpath is all that occurs after ``"<host>/cdmi/"'' in request path.
%% The second argument is a whole request path as one binary.
%% @end
-spec route([binary()],binary()) -> atom().
%% ====================================================================
route([<<"cdmi_capabilities">>| _Rest], _FullPath) -> cdmi_capabilities;
route([<<"cdmi_objectid">>| _Rest], _FullPath) -> cdmi_objectid;
route(_PathList, FullPath) ->
    case binary:last(FullPath) =:= $/ of
        true -> cdmi_container;
        false -> cdmi_object
    end.