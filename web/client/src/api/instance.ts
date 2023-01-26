const baseURL = window.location.origin

export const fetchAPI = async <T>({
  url,
  method,
  params,
  data,
  headers,
  credentials,
  mode,
  cache,
}: {
  url: string;
  method: 'get' | 'post' | 'put' | 'delete' | 'patch';
  params?: any;
  data?: BodyInit;
  responseType?: string;
  headers?: { [key: string]: string };
  credentials?: 'omit' | 'same-origin' | 'include';
  mode?: 'cors' | 'no-cors' | 'same-origin';
  cache?: 'default' | 'no-store' | 'reload' | 'no-cache' | 'force-cache' | 'only-if-cached';
}): Promise<T> => {
  const response = await fetch(
    `${baseURL}${url}` + new URLSearchParams(params),
    {
      method,
      headers,
      credentials,
      mode,
      cache,
      ...(data ? { body: JSON.stringify(data) } : {}),
    },
  );

  return response.json();
};

export default fetchAPI;
