%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions responsible for retrieving  path from request
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_path).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

-type path() :: binary().

%% API
-export([get_path/1, get_path_of_id_request/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file path from cowboy request of type /cdmi/... (works with special characters)
%% @end
%%--------------------------------------------------------------------
-spec get_path(cowboy_req:req()) -> {path(), cowboy_req:req()}.
get_path(Req) ->
    RawPath = cowboy_req:path(Req),
    Path = cowboy_req:path_info(Req),
    JoinedPath = join_filename(Path, RawPath),
    {JoinedPath, Req}.

%%--------------------------------------------------------------------
%% @doc
%% Get file path from cowboy request of type /cdmi/cdmi_object/:id/... (works with special characters)
%% @end
%%--------------------------------------------------------------------
-spec get_path_of_id_request(cowboy_req:req()) -> {path(), cowboy_req:req()}.
get_path_of_id_request(Req) ->
    RawPath = cowboy_req:path(Req),
    Path = cowboy_req:path_info(Req),
    Id = cowboy_req:binding(id, Req),
    IdSize = byte_size(Id),
    <<"/cdmi/cdmi_objectid/", Id:IdSize/binary, RawPathSuffix/binary>> = RawPath,

    case RawPathSuffix of
        <<>> ->
            {<<>>, Req};
        _ ->
            JoinedPath = join_filename(Path, RawPath),
            {JoinedPath, Req}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Join path list and add trailing slash if PathString ends with slash
%% @end
%%--------------------------------------------------------------------
-spec join_filename([path()], path()) -> path().
join_filename(PathList, PathString) ->
    JoinedPath = filename:join([<<"/">> | PathList]),
    case binary:last(PathString) == $/ andalso (not (binary:last(JoinedPath) == $/)) of
        true ->
            <<JoinedPath/binary, "/">>;
        false ->
            JoinedPath
    end.