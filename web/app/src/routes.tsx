import { createBrowserRouter } from "react-router-dom";
import Root from "./library/components/Root/Root";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Root /> ,
  },
]);