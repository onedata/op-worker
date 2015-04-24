%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides set of path processing methods.
%% @end
%% ===================================================================
-module(fslogic_path).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([verify_file_name/1]).
-export([basename/1, split/1, is_space_dir/1]).
-export([ensure_path_begins_with_slash/1]).

%% @todo: absolute_join/1

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Same as {@link filename:split/1} but returned tokens build always relative path.
%% @end
-spec split(Path :: file_meta:path()) -> [binary()].
%%--------------------------------------------------------------------
split(Path) ->
    Bins = binary:split(Path, ?DIRECTORY_SEPARATOR, [global]),
    [Bin || Bin <- Bins, Bin =/= <<"">>].


%%--------------------------------------------------------------------
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
%%--------------------------------------------------------------------
%% @todo: uncomment when implemented
%% -spec get_full_file_name(fslogic:ctx(), FileName :: file_meta:path()) ->
%%     {ok, file_meta:path()} | {error, Reason :: any()} | no_return().
%% get_full_file_name(_CTX, _FileName) ->
%%     ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
%%--------------------------------------------------------------------
-spec verify_file_name(FileName :: file_meta:path()) -> Result when
    Result :: {ok, Tokens :: [binary()]} | {error, wrong_filename}.
verify_file_name(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= <<".">> end, split(FileName)),
    case lists:any(fun(X) -> X =:= <<"..">> end, Tokens) of
        true -> {error, wrong_filename};
        _ -> {ok, Tokens}
    end.


%%--------------------------------------------------------------------
%% @doc Gives file basename from given path
-spec basename(Path :: file_meta:path()) -> file_meta:path().
%% ==================================================================
basename(Path) ->
    case lists:reverse(split(Path)) of
        [Leaf | _] -> Leaf;
        _ -> [?DIRECTORY_SEPARATOR]
    end.


%%--------------------------------------------------------------------
%% @doc Returns true when Path points to space directory (or space root directory)
%%--------------------------------------------------------------------
-spec is_space_dir(Path :: file_meta:path()) -> boolean().
is_space_dir(Path) ->
    case split(Path) of
        [] -> true;
        [?SPACES_BASE_DIR_NAME] -> true;
        [?SPACES_BASE_DIR_NAME , _GroupName] ->  true;
        _ -> false
    end.


%%--------------------------------------------------------------------
%% @doc Ensures that path begins with "/"
%% @end
%%--------------------------------------------------------------------
-spec ensure_path_begins_with_slash(Path :: file_meta:path()) -> file_meta:path().
ensure_path_begins_with_slash(<<"/", _R/binary>> = Path) -> Path;
ensure_path_begins_with_slash(Path) -> <<"/", Path/binary>>.
