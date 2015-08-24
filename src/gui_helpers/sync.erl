%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Developer modules that allows for easy recompilation and reload of
%%% erlang modules, erlyDTL templates and static GUI files.
%%% @end
%%%-------------------------------------------------------------------
-module(sync).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

-define(INFO_MSG(_Format), ?INFO_MSG(_Format, [])).
-define(INFO_MSG(_Format, _Args),
    io:format("[SYNC] ~s~n", [lists:flatten(io_lib:fwrite(_Format, _Args))])
).

%% ETS name that holds md5 checksums of files
-define(MD5_ETS, md5_ets).

%% Resolves target release directory based on rebar.config and reltool.config.
-define(TARGET_RELEASE_LOCATION(_ProjectDir),
    begin
        {ok, _RebarConfig} = file:consult(filename:join([_ProjectDir, "rebar.config"])),
        [_RelDir] = proplists:get_value(sub_dirs, _RebarConfig),
        {ok, _ReltoolConfig} = file:consult(filename:join([_ProjectDir, _RelDir, "reltool.config"])),
        _ReleaseName = proplists:get_value(target_dir, _ReltoolConfig),
        filename:join([_ProjectDir, _RelDir, _ReleaseName])
    end).

%% Predefined file with config (location relative to including project root).
-define(GUI_CONFIG_LOCATION, "rel/gui.config").

%% API
-export([reset/0, recompile/2]).

%%%===================================================================
%%% API
%%%===================================================================


reset() ->
    case ets:info(?MD5_ETS) of
        undefined ->
            ets:new(?MD5_ETS, [set, protected, named_table, {read_concurrency, true}]);
        _ ->
            ets:delete_all_objects(?MD5_ETS)
    end,
    ?INFO_MSG("Cleared the cache.").


file_md5(FilePath) ->
    {ok, Bin} = file:read_file(FilePath),
    erlang:md5(Bin).


find_all_files(Where, NameRegexp, RelativePaths) ->
    case RelativePaths of
        false ->
            string:tokens(shell_cmd(["find", Where, "-type f -name", "'" ++ NameRegexp ++ "'"]), "\n");
        true ->
            string:tokens(shell_cmd(["cd", Where, "&&", "find . -type f -name", "'" ++ NameRegexp ++ "'"]), "\n")
    end.


should_update(FilePath, CurrentMD5) ->
    case ets:info(?MD5_ETS) of
        undefined ->
            ets:new(?MD5_ETS, [set, protected, named_table, {read_concurrency, true}]),
            ?INFO_MSG("Started new ETS table to track changes in files.");
        _ ->
            ok
    end,
    case ets:lookup(?MD5_ETS, FilePath) of
        [{FilePath, CurrentMD5}] ->
            false;
        _ ->
            true
    end.


update_file_md5(FilePath, CurrentMD5) ->
    ets:insert(?MD5_ETS, {FilePath, CurrentMD5}).


recompile(ProjectDir, [First | _] = DirsToRecompile) when is_list(First) ->
    UpdateErlFilesRes = update_erl_files(ProjectDir, DirsToRecompile),
    UpdateStaticFilesRes = update_static_files(ProjectDir),
    case UpdateErlFilesRes andalso UpdateStaticFilesRes of
        true ->
            ?INFO_MSG("Success!"),
            ok;
        false ->
            ?INFO_MSG("There were errors."),
            error
    end;

recompile(ProjectDir, Dir) ->
    recompile(ProjectDir, [Dir]).


update_erl_files(ProjectDir, DirsToRecompile) ->
    GuiConfigPath = filename:join([ProjectDir, ?GUI_CONFIG_LOCATION]),
    {ok, GuiConfig} = file:consult(GuiConfigPath),
    SourcePagesDir = proplists:get_value(source_pages_dir, GuiConfig),

    FilesToCheck = lists:foldl(
        fun(DirPath, Acc) ->
            Files = find_all_files(filename:join(ProjectDir, DirPath), "*.erl", false),
            Files ++ Acc
        end, [], [SourcePagesDir | DirsToRecompile]),

    _Result = lists:foldl(fun(File, Success) ->
        case Success of
            false ->
                false;
            true ->
                update_erl_file(File, [{i, filename:join(ProjectDir, "include")}, report])
        end
    end, true, FilesToCheck).


update_erl_file(File, CompileOpts) ->
    CurrentMD5 = file_md5(File),
    case should_update(File, CurrentMD5) of
        false ->
            true;
        true ->
            case compile:file(File, CompileOpts) of
                {ok, ModuleName} ->
                    code:purge(ModuleName),
                    code:load_file(ModuleName),
                    ?INFO_MSG("Compiled:    ~s", [filename:basename(File)]),
                    update_file_md5(File, CurrentMD5),
                    true;
                _ ->
                    false
            end
    end.


update_static_files(ProjectDir) ->
    GuiConfigPath = filename:join([ProjectDir, ?GUI_CONFIG_LOCATION]),
    {ok, GuiConfig} = file:consult(GuiConfigPath),
    RelaseStaticFilesDir = proplists:get_value(release_static_files_dir, GuiConfig),
    SourceCommonFilesDir = filename:join([ProjectDir, proplists:get_value(source_common_files_dir, GuiConfig)]),
    SourceCommonFilesDirName = filename:basename(SourceCommonFilesDir),
    SourcePagesDir = filename:join([ProjectDir, proplists:get_value(source_pages_dir, GuiConfig)]),

    % Returns tuples with source file path, and target file path but relative to
    % RelaseStaticFilesDir.
    CommonFileMappings = lists:map(
        fun(File) ->
            {filename:join([SourceCommonFilesDir, File]),
                filename:join([SourceCommonFilesDirName, File])}
        end, find_all_files(SourceCommonFilesDir, "*", true)),

    % Returns tuples with source file path, and target file path but relative to
    % RelaseStaticFilesDir.
    PagesFileMappings = lists:map(
        fun(File) ->
            {filename:join([SourcePagesDir, File]), File}
        end, find_all_files(SourcePagesDir, "*", true)),

    _Result = lists:foldl(fun({SourceFilePath, TargetFile}, Success) ->
        case Success of
            false ->
                false;
            true ->
                case filename:extension(TargetFile) of
                    ".erl" ->
                        % Do not copy erl files
                        Success;
                    ".coffee" ->
                        % Compile coffee files and place js in release
                        update_coffee_script(SourceFilePath,
                            RelaseStaticFilesDir, TargetFile);
                    ".html" ->
                        % Copy html files to static files root
                        update_static_file(SourceFilePath,
                            RelaseStaticFilesDir, filename:basename(SourceFilePath));
                    _ ->
                        % Copy all other files 1:1 (path-wise)
                        update_static_file(SourceFilePath,
                            RelaseStaticFilesDir, TargetFile)
                end
        end
    end, true, CommonFileMappings ++ PagesFileMappings).


update_static_file(SourceFile, RelaseStaticFilesDir, TargetFile) ->
    TargetPath = filename:join(RelaseStaticFilesDir, TargetFile),
    CurrentMD5 = file_md5(SourceFile),
    case should_update(SourceFile, CurrentMD5) of
        false ->
            true;
        true ->
            case shell_cmd(["mkdir -p", filename:dirname(TargetPath)]) of
                [] ->
                    case shell_cmd(["cp -f", SourceFile, TargetPath]) of
                        [] ->
                            ?INFO_MSG("Updated:     ~s", [abs_path(TargetFile)]),
                            update_file_md5(SourceFile, CurrentMD5),
                            true;
                        Other1 ->
                            ?INFO_MSG("Cannot copy ~s: ~s", [TargetFile, Other1]),
                            false
                    end;
                Other2 ->
                    ?INFO_MSG("Cannot create dir ~s: ~s", [filename:dirname(TargetFile), Other2]),
                    false
            end
    end.


update_coffee_script(SourceFile, RelaseStaticFilesDir, TargetFile) ->
    TargetPath = filename:join(RelaseStaticFilesDir, TargetFile),
    TargetDir = filename:dirname(TargetPath),
    CurrentMD5 = file_md5(SourceFile),
    case should_update(SourceFile, CurrentMD5) of
        false ->
            true;
        true ->
            case shell_cmd(["mkdir -p", TargetDir]) of
                [] ->
                    case shell_cmd(["coffee", "-o", TargetDir, "-c", SourceFile]) of
                        [] ->
                            JSFile = filename:rootname(TargetFile) ++ ".js",
                            ?INFO_MSG("Compiled:    ~s -> ~s",
                                [abs_path(TargetFile), abs_path(JSFile)]),
                            update_file_md5(SourceFile, CurrentMD5),
                            true;
                        Other ->
                            ?INFO_MSG("Cannot compile ~s: ~s", [SourceFile, Other]),
                            false
                    end;
                Other2 ->
                    ?INFO_MSG("Cannot create dir ~s: ~s", [TargetDir, Other2]),
                    false
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Performs a shell call given a list of arguments.
%% @end
%%--------------------------------------------------------------------
-spec shell_cmd([string()]) -> string().
shell_cmd(List) ->
    os:cmd(string:join(List, " ")).


abs_path(FilePath) ->
    filename:absname_join("/", FilePath).
