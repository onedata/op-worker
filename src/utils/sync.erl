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

%% Predefined file with config used by this escript (location relative to including project root).
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
    CopyStaticFilesRes = copy_static_files(ProjectDir),
    CompileCoffeeScriptsRes = compile_coffe_scripts(ProjectDir),
    CompileTemplatesRes = compile_templates(ProjectDir),
    case UpdateErlFilesRes andalso
        CopyStaticFilesRes andalso
        CompileCoffeeScriptsRes andalso
        CompileTemplatesRes of
        true ->
            ?INFO_MSG("Success!"),
            ok;
        false ->
            ?INFO_MSG("There were compilation errors."),
            error
    end;

recompile(ProjectDir, Dir) ->
    recompile(ProjectDir, [Dir]).


update_erl_files(ProjectDir, DirsToRecompile) ->
    FilesToCheck = lists:foldl(
        fun(DirPath, Acc) ->
            Files = find_all_files(filename:join(ProjectDir, DirPath), "*.erl", false),
            Files ++ Acc
        end, [], DirsToRecompile),
    _Result = lists:foldl(fun(File, Success) ->
        case Success of
            false ->
                false;
            true ->
                CurrentMD5 = file_md5(File),
                case should_update(File, CurrentMD5) of
                    false ->
                        Success;
                    true ->
                        case compile:file(File, [{i, filename:join(ProjectDir, "include")}, report]) of
                            {ok, ModuleName} ->
                                code:purge(ModuleName),
                                code:load_file(ModuleName),
                                ?INFO_MSG("Compiled:    ~s", [filename:basename(File)]),
                                update_file_md5(File, CurrentMD5),
                                true;
                            _ ->
                                false
                        end
                end
        end
    end, true, FilesToCheck).


copy_static_files(ProjectDir) ->
    GuiConfigPath = filename:join([ProjectDir, ?GUI_CONFIG_LOCATION]),
    {ok, GuiConfig} = file:consult(GuiConfigPath),
    SourceStaticFilesDir = filename:join([ProjectDir, proplists:get_value(source_static_files_dir, GuiConfig)]),
    RelaseStaticFilesDir = proplists:get_value(release_static_files_dir, GuiConfig),
    FilesToCheck = find_all_files(SourceStaticFilesDir, "*", true),
    _Result = lists:foldl(fun(File, Success) ->
        case Success of
            false ->
                false;
            true ->
                FileInSrc = filename:join([SourceStaticFilesDir, File]),
                FileTarget = filename:join([RelaseStaticFilesDir, File]),
                CurrentMD5 = file_md5(FileInSrc),
                case should_update(FileInSrc, CurrentMD5) of
                    false ->
                        Success;
                    true ->
                        case shell_cmd(["cp -f", FileInSrc, FileTarget]) of
                            [] ->
                                ?INFO_MSG("Updated:     ~s", [filename:join([RelaseStaticFilesDir, File])]),
                                update_file_md5(FileInSrc, CurrentMD5),
                                true;
                            Other ->
                                ?INFO_MSG("Cannot copy ~s: ~s", [File, Other]),
                                false
                        end
                end
        end
    end, true, FilesToCheck).


compile_coffe_scripts(ProjectDir) ->
    GuiConfigPath = filename:join([ProjectDir, ?GUI_CONFIG_LOCATION]),
    {ok, GuiConfig} = file:consult(GuiConfigPath),
    SourceCoffeeFilesDir = filename:join([ProjectDir, proplists:get_value(source_coffee_files_dir, GuiConfig)]),
    RelaseStaticFilesDir = proplists:get_value(release_static_files_dir, GuiConfig),
    ReleaseCoffeeFilesDir = filename:join([RelaseStaticFilesDir, proplists:get_value(release_coffee_files_dir, GuiConfig)]),
    FilesToCheck = find_all_files(SourceCoffeeFilesDir, "*.coffee", false),
    _Result = lists:foldl(fun(File, Success) ->
        case Success of
            false ->
                false;
            true ->
                CurrentMD5 = file_md5(File),
                case should_update(File, CurrentMD5) of
                    false ->
                        Success;
                    true ->
                        case shell_cmd(["coffee", "-o", ReleaseCoffeeFilesDir, "-c", File]) of
                            [] ->
                                ?INFO_MSG("Compiled:    ~s", [filename:basename(File)]),
                                update_file_md5(File, CurrentMD5),
                                true;
                            Other ->
                                ?INFO_MSG("Cannot compile ~s: ~s", [File, Other]),
                                false
                        end
                end
        end
    end, true, FilesToCheck).


compile_templates(ProjectDir) ->
    GuiConfigPath = filename:join([ProjectDir, ?GUI_CONFIG_LOCATION]),
    {ok, GuiConfig} = file:consult(GuiConfigPath),
    SourceTemplateFilesDir = filename:join([ProjectDir, proplists:get_value(source_template_files_dir, GuiConfig)]),
    TemplateFilesExt = proplists:get_value(source_template_files_ext, GuiConfig),
    ReleaseTemplateFilesSuffix = proplists:get_value(release_template_file_suffix, GuiConfig),
    FilesToCheck = find_all_files(SourceTemplateFilesDir, "*" ++ TemplateFilesExt, false),
    _Result = lists:foldl(fun(File, Success) ->
        case Success of
            false ->
                false;
            true ->
                CurrentMD5 = file_md5(File),
                case should_update(File, CurrentMD5) of
                    false ->
                        Success;
                    true ->
                        ModuleNameStr = filename:basename(filename:rootname(File)) ++
                            ReleaseTemplateFilesSuffix,
                        case erlydtl:compile(File, list_to_atom(ModuleNameStr)) of
                            ok ->
                                ?INFO_MSG("Compiled:    ~s", [filename:basename(File)]),
                                update_file_md5(File, CurrentMD5),
                                true;
                            Other ->
                                ?INFO_MSG("Cannot compile ~s: ~s", [File, Other]),
                                false
                        end
                end
        end
    end, true, FilesToCheck).




%%--------------------------------------------------------------------
%% @doc
%% @private
%% Performs a shell call given a list of arguments.
%% @end
%%--------------------------------------------------------------------
-spec shell_cmd([string()]) -> string().
shell_cmd(List) ->
    os:cmd(string:join(List, " ")).