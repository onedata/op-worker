%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides access to cdmi capabilities
%%% which are used to discover operation that can be performed.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_capabilities).

-include("http/http_common.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").

%% API
-export([
    root_capabilities/1,
    container_capabilities/1,
    dataobject_capabilities/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec root_capabilities([Opt :: binary()]) -> maps:map().
root_capabilities(Opts) ->
    lists:foldl(
        fun
            (<<"objectType">>, Acc) ->
                Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
            (<<"objectID">>, Acc) ->
                Acc#{<<"objectID">> => ?root_capability_id};
            (<<"objectName">>, Acc) ->
                Acc#{<<"objectName">> => ?root_capability_path};
            (<<"parentURI">>, Acc) ->
                Acc;
            (<<"parentID">>, Acc) ->
                Acc;
            (<<"capabilities">>, Acc) ->
                Acc#{<<"capabilities">> => ?root_capability_map};
            (<<"childrenrange">>, Acc) ->
                Acc#{<<"childrenrange">> => <<"0-1">>}; %todo hardcoded childrens, when adding childrenranges or new capabilities, this has to be changed
            (<<"children">>, Acc) ->
                Acc#{<<"children">> => [
                    filepath_utils:ensure_ends_with_slash(filename:basename(?container_capability_path)),
                    filepath_utils:ensure_ends_with_slash(filename:basename(?dataobject_capability_path))
                ]};
            (_, Acc) ->
                Acc
        end,
        #{},
        Opts
    ).


-spec container_capabilities([Opt :: binary()]) -> maps:map().
container_capabilities(Opts) ->
    lists:foldl(
        fun
            (<<"objectType">>, Acc) ->
                Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
            (<<"objectID">>, Acc) ->
                Acc#{<<"objectID">> => ?container_capability_id};
            (<<"objectName">>, Acc) ->
                Acc#{<<"objectName">> => filepath_utils:ensure_ends_with_slash(filename:basename(?container_capability_path))};
            (<<"parentURI">>, Acc) ->
                Acc#{<<"parentURI">> => ?root_capability_path};
            (<<"parentID">>, Acc) ->
                Acc#{<<"parentID">> => ?root_capability_id};
            (<<"capabilities">>, Acc) ->
                Acc#{<<"capabilities">> => ?container_capability_list};
            (<<"childrenrange">>, Acc) ->
                Acc;
            (<<"children">>, Acc) ->
                Acc#{<<"children">> => []};
            (_, Acc) ->
                Acc
        end,
        #{},
        Opts
    ).


-spec dataobject_capabilities([Opt :: binary()]) -> maps:map().
dataobject_capabilities(Opts) ->
    lists:foldl(
        fun
            (<<"objectType">>, Acc) ->
                Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
            (<<"objectID">>, Acc) ->
                Acc#{<<"objectID">> => ?dataobject_capability_id};
            (<<"objectName">>, Acc) ->
                Acc#{<<"objectName">> => filepath_utils:ensure_ends_with_slash(filename:basename(?dataobject_capability_path))};
            (<<"parentURI">>, Acc) ->
                Acc#{<<"parentURI">> => ?root_capability_path};
            (<<"parentID">>, Acc) ->
                Acc#{<<"parentID">> => ?root_capability_id};
            (<<"capabilities">>, Acc) ->
                Acc#{<<"capabilities">> => ?dataobject_capability_list};
            (<<"childrenrange">>, Acc) ->
                Acc;
            (<<"children">>, Acc) ->
                Acc#{<<"children">> => []};
            (_, Acc) ->
                Acc
        end,
        #{},
        Opts
    ).
