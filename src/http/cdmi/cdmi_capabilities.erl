%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides access to cdmi capabilities which are
%%% used to discover operations that can be performed.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_capabilities).
-author("Piotr Ociepka").
-author("Bartosz Walkowicz").

-include("http/cdmi.hrl").

%% API
-export([
    get_cdmi/2
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets requested info about requestet object capabilities.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {binary(), cowboy_req:req(), cdmi_handler:cdmi_req()}.
get_cdmi(Req, #cdmi_req{resource = {capabilities, CapType},
    options = Options
} = CdmiReq) ->
    NonEmptyOpts = utils:ensure_defined(
        Options, [], ?DEFAULT_CAPABILITIES_OPTIONS
    ),
    Capabilities = case CapType of
        root -> root_capabilities(NonEmptyOpts);
        container -> container_capabilities(NonEmptyOpts);
        dataobject -> dataobject_capabilities(NonEmptyOpts)
    end,
    {json_utils:encode(Capabilities), Req, CdmiReq}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec root_capabilities(RequestedOptions :: [binary()]) -> map().
root_capabilities(RequestedOptions) ->
    lists:foldl(fun
        (<<"objectType">>, Acc) ->
            Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
        (<<"objectID">>, Acc) ->
            Acc#{<<"objectID">> => ?ROOT_CAPABILITY_ID};
        (<<"objectName">>, Acc) ->
            Acc#{<<"objectName">> => <<?ROOT_CAPABILITY_PATH>>};
        (<<"capabilities">>, Acc) ->
            Acc#{<<"capabilities">> => ?ROOT_CAPABILITY_MAP};
        (<<"childrenrange">>, Acc) ->
            %TODO VFS-7290 hardcoded children, when adding childrenranges or new capabilities, this has to be changed
            Acc#{<<"childrenrange">> => <<"0-1">>};
        (<<"children">>, Acc) ->
            Acc#{<<"children">> => [
                filepath_utils:ensure_ends_with_slash(filename:basename(<<?CONTAINER_CAPABILITY_PATH>>)),
                filepath_utils:ensure_ends_with_slash(filename:basename(<<?DATAOBJECT_CAPABILITY_PATH>>))
            ]};
        (_, Acc) ->
            Acc
    end, #{}, RequestedOptions).


%% @private
-spec container_capabilities(RequestedOptions :: [binary()]) -> map().
container_capabilities(RequestedOptions) ->
    lists:foldl(fun
        (<<"objectType">>, Acc) ->
            Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
        (<<"objectID">>, Acc) ->
            Acc#{<<"objectID">> => ?CONTAINER_CAPABILITY_ID};
        (<<"objectName">>, Acc) ->
            Acc#{<<"objectName">> => filepath_utils:ensure_ends_with_slash(
                filename:basename(<<?CONTAINER_CAPABILITY_PATH>>)
            )};
        (<<"parentURI">>, Acc) ->
            Acc#{<<"parentURI">> => <<?ROOT_CAPABILITY_PATH>>};
        (<<"parentID">>, Acc) ->
            Acc#{<<"parentID">> => ?ROOT_CAPABILITY_ID};
        (<<"capabilities">>, Acc) ->
            Acc#{<<"capabilities">> => ?CONTAINER_CAPABILITY_MAP};
        (<<"children">>, Acc) ->
            Acc#{<<"children">> => []};
        (_, Acc) ->
            Acc
    end, #{}, RequestedOptions).


%% @private
-spec dataobject_capabilities(RequestedOptions :: [binary()]) -> map().
dataobject_capabilities(RequestedOptions) ->
    lists:foldl(fun
        (<<"objectType">>, Acc) ->
            Acc#{<<"objectType">> => <<"application/cdmi-capability">>};
        (<<"objectID">>, Acc) ->
            Acc#{<<"objectID">> => ?DATAOBJECT_CAPABILITY_ID};
        (<<"objectName">>, Acc) ->
            Acc#{<<"objectName">> => filepath_utils:ensure_ends_with_slash(
                filename:basename(<<?DATAOBJECT_CAPABILITY_PATH>>)
            )};
        (<<"parentURI">>, Acc) ->
            Acc#{<<"parentURI">> => <<?ROOT_CAPABILITY_PATH>>};
        (<<"parentID">>, Acc) ->
            Acc#{<<"parentID">> => ?ROOT_CAPABILITY_ID};
        (<<"capabilities">>, Acc) ->
            Acc#{<<"capabilities">> => ?DATAOBJECT_CAPABILITY_MAP};
        (<<"children">>, Acc) ->
            Acc#{<<"children">> => []};
        (_, Acc) ->
            Acc
    end, #{}, RequestedOptions).
