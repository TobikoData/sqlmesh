export function toSingular(noun: string = ''): string {
  const endsWith = (suffix: string = '') => noun.endsWith(suffix);
  const slice = (start: number, end: number) => noun.slice(start, end);

  if (endsWith("s")) return slice(0, -1);
  if (endsWith("ies")) return slice(0, -3) + "y";
  if (endsWith("es")) return slice(0, -2);
  if (endsWith("ves")) return slice(0, -3) + "f";
  
  return noun;
}