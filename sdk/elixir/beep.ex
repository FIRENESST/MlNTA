defmodule Beep do
  @moduledoc """
  Elixir client for BEEP backends via the beepgw HTTP gateway, using OTP's built-in `:httpc`
  (no dependencies). Test: test.exs.

      {:ok, %{status: 200, body: body}} = Beep.call(:post, "/orders", [{"content-type", "application/json"}], ~s({"id":7}))

  Phoenix/Plug apps can be exposed on BEEP with `beepgw expose --backend http://127.0.0.1:4000`.
  """

  def gateway, do: System.get_env("BEEP_GATEWAY", "http://127.0.0.1:8080")

  @spec call(atom | String.t(), String.t(), [{String.t(), String.t()}], iodata | nil, keyword) ::
          {:ok, %{status: integer, headers: list, body: binary}} | {:error, term}
  def call(method, target, headers \\ [], body \\ nil, opts \\ []) do
    :inets.start()
    method = method |> to_string() |> String.downcase() |> String.to_atom()
    hdrs =
      headers
      |> maybe_add("x-beep-upstream", opts[:upstream])
      |> maybe_add("x-beep-timeout-ms", opts[:timeout_ms] && Integer.to_string(opts[:timeout_ms]))

    {ctype, rest} = pop_content_type(hdrs)
    url = String.to_charlist(gateway() <> target)
    chdrs = Enum.map(rest, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)

    request =
      if body == nil and method in [:get, :head, :delete, :options],
        do: {url, chdrs},
        else: {url, chdrs, String.to_charlist(ctype), IO.iodata_to_binary(body || "")}

    case :httpc.request(method, request, [timeout: 120_000], body_format: :binary) do
      {:ok, {{_, status, _}, resp_headers, resp_body}} ->
        resp_headers = Enum.map(resp_headers, fn {k, v} -> {to_string(k), to_string(v)} end)

        case List.keyfind(resp_headers, "x-beep-error-code", 0) do
          {_, code} -> {:error, {:beep, String.to_integer(code), status, resp_body}}
          nil -> {:ok, %{status: status, headers: resp_headers, body: resp_body}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_add(h, _k, nil), do: h
  defp maybe_add(h, k, v), do: [{k, v} | h]

  defp pop_content_type(h) do
    case Enum.split_with(h, fn {k, _} -> String.downcase(k) == "content-type" end) do
      {[{_, ct} | _], rest} -> {ct, rest}
      {[], rest} -> {"application/octet-stream", rest}
    end
  end
end
